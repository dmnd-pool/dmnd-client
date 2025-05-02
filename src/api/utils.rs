use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};

pub(super) async fn get_cpu_and_memory_usage() -> (f32, u64) {
    let mut system = System::new_all();

    // First refresh to get initial values
    system.refresh_processes_specifics(
        ProcessesToUpdate::All,
        true,
        ProcessRefreshKind::nothing().with_cpu().with_memory(),
    );

    // Wait for a measurable interval
    tokio::time::sleep(sysinfo::MINIMUM_CPU_UPDATE_INTERVAL).await;

    // Second refresh to get the difference
    system.refresh_processes_specifics(
        ProcessesToUpdate::All,
        true,
        ProcessRefreshKind::nothing().with_cpu().with_memory(),
    );

    let pid = std::process::id();
    if let Some(process) = system.process(Pid::from_u32(pid)) {
        let cpu_usage = process.cpu_usage();
        let cpu_nums = system.cpus().len() as f32; // get the number of cpu

        let normalized_cpu_usage = if cpu_nums > 0.0 {
            cpu_usage / cpu_nums
        } else {
            0.0
        };

        let memory = process.memory();
        (normalized_cpu_usage, memory)
    } else {
        (0.0, 0)
    }
}
