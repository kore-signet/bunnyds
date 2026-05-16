# bunnyds ／(≧ x ≦)＼

a tiny rust/asyncio executor for the 3ds. a work-in-progress!

### why?

the 3ds is a cooperatively multi-tasked system: threads will hog the cpu until they explicitly yield. 
this is, coincidentally, the exact model asyncio is based on! so it's a natural fit :3

### running an async program

```rust
use std::time::Duration;
use ctru::prelude::*;
use ds_ipc::DSResult;

async fn async_main() -> DSResult<()> {
    bunnyds::sleep(Duration::from_secs(5)).await;
    println!("task done!");

    Ok(())
}

fn main() {
    let mut apt = Apt::new().unwrap();
    let gfx = Gfx::new().unwrap();
    let _console = Console::new(gfx.top_screen.borrow_mut());
    apt.set_app_cpu_time_limit(30).unwrap();

    let runtime = bunnyds::RuntimeConfiguration {
        fs_workers: 2,
        enable_soc: true,
    }
    .run();

    if let Err(e) = runtime.block_on(async_main()) {
        println!("ended in error: {e}");
    }

    while apt.main_loop() {
        std::thread::sleep(Duration::from_secs(5));
    }
}
```

### basics

```rust
// spawning tasks
bunnyds::spawn(async {
    bunnyds::sleep(Duration::from_secs(5)).await;
    println!("hi!");
});

// spawning *blocking* tasks - this spawns normal 3ds threads, so you need to be extra careful to not hog the cpu!
bunnyds::spawn_blocking(|| {
    std::thread::sleep(Duration::from_secs(5));
    println!("a blocking hi!");
});
```

### tcp networking
```rust

let server = AsyncTcpSocket::bind("0.0.0.0:3027")?;
let client = server.accept().await?;

let mut data_received = [0u8; 512];
let read_bytes = client.recv(&mut data_received).await?;
client.send(&data_received[..read_bytes]).await?;
```

### file IO
```rust
let mut file = bunnyds::fs::open(widestring::u16cstr!("/file.txt"), OpenFlags::READ | OpenFlags::CREATE | OpenFlags::WRITE)?;
file.write(0, b"this is 16bytes!").await?;
let mut data = [0u8; 16];
let bytes_read = file.read(0, &mut data[..]).await?;
println!("{}", String::from_utf8_lossy(&data[..bytes_read]));
```

### synchronization/channels
channels from crates like `thingbuf` should work! there's a couple special primitives, though:
```rust
// oneshot channels
let (tx, rx) = bunnyds::sync::oneshot::<u8>();
bunnyds::spawn(async move {
    bunnyds::sleep(Duration::from_secs(3)).await;
    tx.send(5);
});
rx.await

// mutexes
let locked = Arc::new(bunnyds::sync::DSMutex::new(0u8));
let locked_tx = Arc::clone(&locked);
bunnyds::spawn_blocking(move || {
    *locked_tx.lock_sync() = 1;
}); // works with tasks, too!
bunnyds::sleep(Duration::from_secs(3)).await;
println!("{}", locked.lock().await);

// notifications (specialized oneshot channels)
let (tx, rx) = bunnyds::sync::notification();
bunnyds::spawn(async move {
    bunnyds::sleep(Duration::from_secs(3)).await;
    tx.notify();
});
rx.await
```