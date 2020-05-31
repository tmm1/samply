use super::kernel_error::{self, IntoResult};
use mach;
use mach::message::mach_msg_type_number_t;
use mach::port::mach_port_t;
use mach::task::{task_info, task_resume, task_suspend};
use mach::task_info::{task_info_t, TASK_DYLD_INFO};
use mach::thread_act::{thread_get_state, thread_resume, thread_suspend};
use mach::thread_status::{thread_state_t, x86_THREAD_STATE64};
use mach::traps::mach_task_self;
use mach::vm::{mach_vm_deallocate, mach_vm_read, mach_vm_remap};
use mach::vm_inherit::VM_INHERIT_SHARE;
use mach::vm_page_size::{mach_vm_trunc_page, vm_page_size};
use mach::vm_prot::{vm_prot_t, VM_PROT_NONE};
use mach::vm_types::{mach_vm_address_t, mach_vm_size_t};
use std::cmp::Ordering;
use std::mem;
use std::ptr;
use uuid::Uuid;

use mach::structs::x86_thread_state64_t;

use crate::dyld_bindings;
use dyld_bindings::{
    dyld_all_image_infos, dyld_image_info, load_command, mach_header_64, segment_command_64,
    uuid_command,
};

pub const TASK_DYLD_INFO_COUNT: mach_msg_type_number_t = 5;

#[derive(Debug, Clone)]
pub struct ThreadInfo {
    pub tid: u64,
    pub name: String,
    pub backtrace: Option<Vec<u64>>,
}

#[derive(Debug, Clone)]
pub struct DyldInfo {
    pub file: String,
    pub address: u64,
    pub vmsize: u64,
    pub uuid: Option<Uuid>,
}

pub fn get_dyld_info(task: mach_port_t) -> kernel_error::Result<Vec<DyldInfo>> {
    unsafe { task_suspend(task) }.into_result()?;
    let result = get_dyld_info_with_suspended_task(task);
    let _ = unsafe { task_resume(task) };
    result
}

pub fn get_dyld_info_with_suspended_task(task: mach_port_t) -> kernel_error::Result<Vec<DyldInfo>> {
    // Adapted from rbspy and from the Gecko profiler's shared-libraries-macos.cc.

    let mut vec = Vec::new();

    let mut dyld_info = task_dyld_info {
        all_image_info_addr: 0,
        all_image_info_size: 0,
        all_image_info_format: 0,
    };

    let mut count = TASK_DYLD_INFO_COUNT;
    unsafe {
        task_info(
            task,
            TASK_DYLD_INFO,
            &mut dyld_info as *mut task_dyld_info as task_info_t,
            &mut count,
        )
    }
    .into_result()?;

    let mut memory = ForeignMemory::new(task);

    let (info_array_addr, info_array_count) = {
        let image_infos: &dyld_all_image_infos =
            unsafe { memory.get_type_ref_at_address(dyld_info.all_image_info_addr) }?;
        (
            image_infos.infoArray as usize as u64,
            image_infos.infoArrayCount,
        )
    };

    for image_index in 0..info_array_count {
        let (image_load_address, image_file_path) = {
            let info_array_elem_addr =
                info_array_addr + image_index as u64 * mem::size_of::<dyld_image_info>() as u64;
            let image_info: &dyld_image_info =
                unsafe { memory.get_type_ref_at_address(info_array_elem_addr) }?;
            (
                image_info.imageLoadAddress as usize as u64,
                image_info.imageFilePath as usize as u64,
            )
        };

        let filename = {
            let filename_bytes: &[i8; 512] =
                unsafe { memory.get_type_ref_at_address(image_file_path) }?;
            unsafe { std::ffi::CStr::from_ptr(filename_bytes.as_ptr()) }
                .to_string_lossy()
                .to_string()
        };

        let (header_n_cmds, header_sizeof_cmds) = {
            let header: &mach_header_64 =
                unsafe { memory.get_type_ref_at_address(image_load_address) }?;
            (header.ncmds, header.sizeofcmds)
        };

        let commands_addr = image_load_address + mem::size_of::<mach_header_64>() as u64;
        let commands_range = commands_addr..(commands_addr + header_sizeof_cmds as u64);
        let commands_buffer = memory.get_slice(commands_range)?;

        // Figure out the slide from the __TEXT segment if appropiate
        let mut vmsize: u64 = 0;
        let mut uuid = None;
        let mut offset = 0;
        for _ in 0..header_n_cmds {
            unsafe {
                let command = &commands_buffer[offset] as *const u8 as *const load_command;
                match (*command).cmd {
                    0x19 => {
                        // LC_SEGMENT_64
                        let segcmd = command as *const segment_command_64;
                        if (*segcmd).segname[0..7] == [95, 95, 84, 69, 88, 84, 0] {
                            // This is the __TEXT segment.
                            vmsize = (*segcmd).vmsize;
                        }
                    }
                    0x1b => {
                        // LC_UUID
                        let ucmd = command as *const uuid_command;
                        uuid = Some(Uuid::from_slice(&(*ucmd).uuid).unwrap());
                    }
                    _ => {}
                }
                offset += (*command).cmdsize as usize;
            }
        }
        vec.push(DyldInfo {
            file: filename.clone(),
            address: image_load_address,
            vmsize,
            uuid,
        });
    }
    vec.sort_by_key(|info| info.address);
    Ok(vec)
}

// bindgen seemed to put all the members for this struct as a single opaque blob:
//      (bindgen /usr/include/mach/task_info.h --with-derive-default --whitelist-type task_dyld_info)
// rather than debug the bindgen command, just define manually here
#[repr(C)]
#[derive(Default, Debug)]
pub struct task_dyld_info {
    pub all_image_info_addr: mach_vm_address_t,
    pub all_image_info_size: mach_vm_size_t,
    pub all_image_info_format: mach::vm_types::integer_t,
}

pub fn get_backtrace(
    memory: &mut ForeignMemory,
    thread_act: mach_port_t,
    frames: &mut Vec<u64>,
) -> kernel_error::Result<()> {
    unsafe { thread_suspend(thread_act) }.into_result()?;

    let mut state: x86_thread_state64_t = unsafe { mem::zeroed() };
    let mut count = x86_thread_state64_t::count();
    let res = unsafe {
        thread_get_state(
            thread_act,
            x86_THREAD_STATE64,
            &mut state as *mut _ as thread_state_t,
            &mut count as *mut _,
        )
    }
    .into_result();

    if let Err(err) = res {
        let _ = unsafe { thread_resume(thread_act) };
        return Err(err);
    }

    do_frame_pointer_stackwalk(&state, memory, frames);

    let _ = unsafe { thread_resume(thread_act) };

    Ok(())
}

fn do_frame_pointer_stackwalk(
    initial_state: &x86_thread_state64_t,
    memory: &mut ForeignMemory,
    frames: &mut Vec<u64>,
) {
    frames.push(initial_state.__rip);

    // Do a frame pointer stack walk. Code that is compiled with frame pointers
    // has the following function prologues and epilogues:
    //
    // Function prologue:
    // pushq  %rbp
    // movq   %rsp, %rbp
    //
    // Function epilogue:
    // popq   %rbp
    // ret
    //
    // Functions are called with callq; callq pushes the return address onto the stack.
    // When a function reaches its end, ret pops the return address from the stack and jumps to it.
    // So when a function is called, we have the following stack layout:
    //
    //                                                                     [... rest of the stack]
    //                                                                     ^ rsp           ^ rbp
    //     callq some_function
    //                                                   [return address]  [... rest of the stack]
    //                                                   ^ rsp                             ^ rbp
    //     pushq %rbp
    //                         [caller's frame pointer]  [return address]  [... rest of the stack]
    //                         ^ rsp                                                       ^ rbp
    //     movq %rsp, %rbp
    //                         [caller's frame pointer]  [return address]  [... rest of the stack]
    //                         ^ rsp, rbp
    //     <other instructions>
    //       [... more stack]  [caller's frame pointer]  [return address]  [... rest of the stack]
    //       ^ rsp             ^ rbp
    //
    // So: *rbp is the caller's frame pointer, and *(rbp + 8) is the return address.
    //
    // Or, in other words, the following linked list is built up on the stack:
    // #[repr(C)]
    // struct CallFrameInfo {
    //     previous: *const CallFrameInfo,
    //     return_address: *const c_void,
    // }
    // and rbp is a *const CallFrameInfo.

    let mut frame_ptr = initial_state.__rbp;
    while frame_ptr != 0 && (frame_ptr & 7) == 0 {
        let caller_frame_ptr = match memory.read_u64_at_address(frame_ptr) {
            Ok(val) => val,
            Err(_) => break, // usually KernelError::InvalidAddress
        };
        // The stack grows towards lower addresses, so the caller frame will always
        // be at a higher address than this frame. Make sure this is the case, so
        // that we don't go in circles.
        if caller_frame_ptr <= frame_ptr {
            break;
        }
        let return_address = match memory.read_u64_at_address(frame_ptr + 8) {
            Ok(val) => val,
            Err(_) => break, // usually KernelError::InvalidAddress
        };
        frames.push(return_address);
        frame_ptr = caller_frame_ptr;
    }

    frames.reverse();
}

#[derive(Debug, Clone)]
pub struct ForeignMemory {
    task: mach_port_t,
    data: Vec<VmData>,
}

impl ForeignMemory {
    pub fn new(task: mach_port_t) -> Self {
        Self {
            task,
            data: Vec::new(),
        }
    }

    pub fn clear(&mut self) {
        self.data.clear();
        self.data.shrink_to_fit();
    }

    pub fn read_u64_at_address(&mut self, address: u64) -> kernel_error::Result<u64> {
        let number: &u64 = unsafe { self.get_type_ref_at_address(address) }?;
        Ok(*number)
    }

    fn data_index_for_address(&self, address: u64) -> std::result::Result<usize, usize> {
        self.data.binary_search_by(|d| {
            if d.address_range.start > address {
                Ordering::Greater
            } else if d.address_range.end <= address {
                Ordering::Less
            } else {
                Ordering::Equal
            }
        })
    }

    fn get_data_for_range(
        &mut self,
        address_range: std::ops::Range<u64>,
    ) -> kernel_error::Result<&VmData> {
        let first_byte_addr = address_range.start;
        let last_byte_addr = address_range.end - 1;
        let vm_data = match (
            self.data_index_for_address(first_byte_addr),
            self.data_index_for_address(last_byte_addr),
        ) {
            (Ok(i), Ok(j)) if i == j => &self.data[i],
            (Ok(i), Ok(j)) | (Ok(i), Err(j)) | (Err(i), Ok(j)) | (Err(i), Err(j)) => {
                let start_addr = unsafe { mach_vm_trunc_page(first_byte_addr) };
                let end_addr = unsafe { mach_vm_trunc_page(last_byte_addr) + vm_page_size as u64 };
                let size = end_addr - start_addr;
                let data = VmData::map_from_task(self.task, start_addr, size)?;
                // Replace everything between i and j with the new combined range.
                self.data.splice(i..j, std::iter::once(data));
                &self.data[i]
            }
        };
        Ok(vm_data)
    }

    pub fn get_slice<'a>(
        &'a mut self,
        range: std::ops::Range<u64>,
    ) -> kernel_error::Result<&'a [u8]> {
        let vm_data = self.get_data_for_range(range.clone())?;
        Ok(vm_data.get_slice(range))
    }

    pub unsafe fn get_type_ref_at_address<'a, T>(
        &'a mut self,
        address: u64,
    ) -> kernel_error::Result<&'a T> {
        let end_addr = address + mem::size_of::<T>() as u64;
        let vm_data = self.get_data_for_range(address..end_addr)?;
        Ok(vm_data.get_type_ref(address))
    }
}

#[derive(Debug, Clone)]
struct VmData {
    address_range: std::ops::Range<u64>,
    data: *mut u8,
    data_size: usize,
}

impl VmData {
    #[allow(unused)]
    pub fn read_from_task(
        task: mach_port_t,
        original_address: u64,
        size: u64,
    ) -> kernel_error::Result<Self> {
        let mut data: *mut u8 = ptr::null_mut();
        let mut data_size: usize = 0;
        unsafe {
            mach_vm_read(
                task,
                original_address,
                size,
                mem::transmute(&mut data),
                mem::transmute(&mut data_size),
            )
        }
        .into_result()?;

        Ok(Self {
            address_range: original_address..(original_address + data_size as u64),
            data,
            data_size,
        })
    }

    pub fn map_from_task(
        task: mach_port_t,
        original_address: u64,
        size: u64,
    ) -> kernel_error::Result<Self> {
        let mut data: *mut u8 = ptr::null_mut();
        let mut cur_protection: vm_prot_t = VM_PROT_NONE;
        let mut max_protection: vm_prot_t = VM_PROT_NONE;
        unsafe {
            mach_vm_remap(
                mach_task_self(),
                mem::transmute(&mut data),
                size,
                0,
                1, /* anywhere: true */
                task,
                original_address,
                0,
                mem::transmute(&mut cur_protection),
                mem::transmute(&mut max_protection),
                VM_INHERIT_SHARE,
            )
        }
        .into_result()?;

        Ok(Self {
            address_range: original_address..(original_address + size),
            data,
            data_size: size as usize,
        })
    }

    pub fn get_slice<'a>(&'a self, address_range: std::ops::Range<u64>) -> &'a [u8] {
        assert!(self.address_range.start <= address_range.start);
        assert!(self.address_range.end >= address_range.end);
        let offset = address_range.start - self.address_range.start;
        let len = address_range.end - address_range.start;
        unsafe { std::slice::from_raw_parts(self.data.offset(offset as isize), len as usize) }
    }

    pub unsafe fn get_type_ref<'a, T>(&'a self, address: u64) -> &'a T {
        assert!(address % mem::align_of::<T>() as u64 == 0);
        let range = address..(address + mem::size_of::<T>() as u64);
        let slice = self.get_slice(range);
        assert!(slice.len() == mem::size_of::<T>());
        &*(slice.as_ptr() as *const T)
    }
}

impl Drop for VmData {
    fn drop(&mut self) {
        let _ = unsafe {
            mach_vm_deallocate(
                mach_task_self(),
                self.data as *mut _ as _,
                self.data_size as _,
            )
        };
    }
}
