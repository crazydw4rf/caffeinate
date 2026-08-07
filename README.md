# caffeinate-rs

Just a simple program to prevent your machine from going to sleep while playing
audio/video.
Only supports MPRIS media players for now.

## How to

Clone this repository.

```bash
git clone https://github.com/crazydw4rf/caffeinate-rs
cd caffeinate-rs
```

### Prerequisites

- [ ] Rust toolchain (you can install it with [rustup](<https://rustup.rs()>))

### How to build

```bash
cargo build --release
```

### How to install

```bash
cargo install --path .
```

### How to run

```bash
caffeinate
```

## TODO

- [ ] allow detecting non-mpris media player
- [ ] lock file
- [ ] waybar support
- [ ] write proper docs
