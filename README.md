# Dropbox to Box Migration Using Rclone

This script copies files directly from Dropbox to Box using rclone. It streams data through your machine without storing it locally.

---

## Prerequisites

1. Install rclone:

```bash
brew install rclone
```

2. Configure Remotes

```bash
rclone config
```

   - Choose `n` for new remote.
   - Name it `saoa-dropbox`.
   - Select Dropbox as the provider (14).
   - Follow OAuth prompts.
   - Repeat and create a remote named `box` (7).

1. Configuration

Edit the script to set these two variables:

```bash
DROPBOX_PATH="/your/source/path"      # Dropbox folder to migrate
BOX_PATH="/your/target/path"          # Box destination folder

4. (Optional) Test access:

```bash
rclone ls dropbox:/
rclone ls box:/
```

---

## Usage

1. Run the script:

```bash
./migrate_dropbox.sh
```

2. Monitor progress:
   - Output appears in the terminal.
   - Logs saved in `rclone_migration_YYYYMMDD_HHMMSS.log`.

---

## Customization

- Adjust bandwidth limit by editing `BANDWIDTH` in the script.
- Change concurrency by modifying `TRANSFERS` and `CHECKERS`.

---

## Notes

- Requires Dropbox and Box account access with proper permissions.
- Files are streamed and not stored locally.
- Resumable if interrupted: rclone skips already copied files.
