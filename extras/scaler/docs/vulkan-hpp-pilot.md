# Vulkan-Hpp CPU runner pilot

Draft for James Riordon and the Vulkan-Hpp maintainers, 16 October 2026.

Run a small, manually triggered Vulkan-Hpp build on ephemeral GCE Linux and Windows
VMs using the Slang scaler. Start with one CPU-only VM per platform, no warm VMs,
and repository-scoped scale sets. Keep existing CI and required checks in place
while measuring whether this is useful for Vulkan-Hpp.

At the source revision linked below, Vulkan-Hpp uses GitHub-hosted Ubuntu,
Windows, and macOS runners; none of its five workflows targets self-hosted GCP
runners. This pilot introduces GCP capacity. The project and image families
listed as prerequisites below are resources to prepare, not confirmed existing
Vulkan-Hpp infrastructure.

## 1. Prerequisites and decisions

**The scaler deployment steps assume a GCP project and Vulkan-Hpp-specific Linux
and Windows disk image families are ready.** If those images do not exist yet,
preparing and validating them is the first prerequisite. The names below are
proposed names, not provisioned resources.

Slang had a head start: its Linux and Windows build environments already existed
on static VMs, set up by others before Janne's scaler work. We took snapshots of
those existing runners and used them to create reusable images in image families.
We did not build the initial Linux and Windows environments from scratch as part
of adopting the scaler. Vulkan-Hpp needs to prepare its own initial images for
both operating systems, including its build tools and the Actions runner; the
scaler does not install that environment onto a stock OS image.

Before starting, James and the maintainer should fill in:

| Item                 | Pilot value                                                          |
| -------------------- | -------------------------------------------------------------------- |
| GitHub repository    | `https://github.com/KhronosGroup/Vulkan-Hpp`                         |
| GCP project          | `<existing Vulkan-Hpp pilot project ID>`                             |
| Image project        | `<project containing the existing image families>`                   |
| Linux image family   | `vulkan-hpp-linux-cpu` or the agreed existing name                   |
| Windows image family | `vulkan-hpp-windows-cpu` or the agreed existing name                 |
| Zone                 | One agreed zone, for example `us-central1-a`                         |
| Subnet               | An existing subnet in that zone's region, with outbound access       |
| Control host         | `vulkan-hpp-scaler`, Linux amd64 with systemd                        |
| Worker sizing        | Start with `e2-standard-4`; review RAM and disk needs                |
| Capacity             | One Linux worker plus one Windows worker; zero warm workers          |
| Owners               | GCP/billing owner, GitHub administrator, and pilot maintainer        |
| Trial                | Suggested: ten successful jobs per platform, including a small burst |

The GCP project needs billing, Compute Engine API, CPU/disk/IP quota, and a budget
alert. A budget alert does not enforce a spending cap. Network policy must allow
outbound HTTPS to GitHub, Actions artifact endpoints, runner downloads, and GCP
APIs. Workers do not need inbound SSH/RDP for Actions; use the project's approved
administrative access path for diagnosis. Private-only workers need suitable NAT.

### Image contract

Both image families must contain x64 operating systems supported by the Actions
runner, the GCE guest environment that executes metadata startup scripts, and an
unconfigured current Actions runner. Do not bake GitHub credentials, registered
runner services, cached cloud credentials, or personal working directories into
the images. The scaler supplies the one-use JIT registration data at VM creation.

| Linux image                                                                   | Windows image                                                                                                           |
| ----------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| Ubuntu 24.04 x64 is the proposed baseline                                     | Windows Server 2022 x64 is the proposed baseline                                                                        |
| Runner at `/home/runner/actions-runner`, owned by a non-root `runner` user    | Runner at `C:\actions-runner`                                                                                           |
| GCC/G++ 13, Ninja, Git, CMake 3.24 or newer, Bash, curl, tar, coreutils, sudo | VS 2022 Build Tools with C++ x64 tools and Windows SDK, Git for Windows including Bash, CMake 3.24 or newer, PowerShell |
| Runner native dependencies installed                                          | Build tools available to the account used by the GCE startup script                                                     |

These baselines are choices for the pilot, not a complete replacement for the
upstream compiler matrix. A CPU-only image needs no NVIDIA driver or GPU. The
Linux startup log may still warn that `nvidia-smi` or Docker is unavailable; neither
is required by the supplied workflow. The existing Linux startup script also
configures `/var/cores` and `kernel.core_pattern`; this Slang behavior currently
runs on every worker, including CPU workers.

The workflow uses the `generator-build`, `generator-run`, and `tests` presets,
recursive submodules, Debug/Release configurations, and C++17/C++20. Vulkan-Hpp's
CTest setup uses `ctest --build-and-test` to compile test projects. This pilot
does not execute Vulkan rendering workloads or demonstrate GPU runtime coverage.
Samples, x86, C++ modules, other compilers, and the full standard matrix can follow
after the first two pools work.

### Preparing the first images if needed

In GCP, "baking an image" can start with an ordinary temporary VM:

1. Create one Linux builder VM and one Windows builder VM from Google's public
   OS images, using the proposed baselines above. Install the tools in the image
   contract and record their versions and installation steps.
2. Install the Actions runner at the expected path, but leave it unregistered.
   Check that the toolchain works under the account that will run jobs, rather
   than only in an administrator's interactive session.
3. Remove temporary credentials, personal data, and competing runner services or
   startup tasks. For Linux, shut down the builder cleanly. For Windows, follow
   Google's preparation procedure and run `GCESysprep` as administrator; it
   generalizes Windows and shuts the VM down. Review local accounts as well:
   Sysprep does not remove all account credentials. See Google's
   [Windows image preparation instructions](https://docs.cloud.google.com/compute/docs/instances/windows/creating-windows-os-image).
4. In Compute Engine's **Images** page, create a custom image from each prepared
   boot disk, with a unique versioned name and the corresponding family name,
   such as `vulkan-hpp-linux-cpu` or `vulkan-hpp-windows-cpu`. A snapshot is also
   a supported source, but is not required when using the stopped builder disk.
   See Google's [custom image creation guide](https://docs.cloud.google.com/compute/docs/images/create-custom).
5. Boot fresh test VMs from the resulting images and verify the toolchain,
   unregistered runner, permissions, and guest startup support. Then run the
   scaler pilot below to validate JIT registration and job execution. Remove
   temporary test/builder VMs and their disks when no longer needed.

An image family groups successive image versions; it is not a running VM or an
image-building service. Keep the installation steps so later versions can be
rebuilt. Packer can automate this later, but the existing Slang Windows GPU
Packer recipe is not a ready-made Vulkan-Hpp CPU image builder.

## 2. Prepare authentication and the control host

Use a dedicated scaler service account on the control VM, with permissions to
read instance templates, create/list/delete worker instances and disks, use the
selected subnet/images, and read operation status. On a dedicated pilot project,
`roles/compute.instanceAdmin.v1` is a practical starting role; have the GCP owner
check image and Shared VPC access separately. The workers below have **no attached
service account**, so a build cannot use the control host's Compute credentials.
Do not attach the control identity to a worker template.

Provision the control host through the project's normal GCE process: a small
Ubuntu amd64 VM (for example `e2-small`), systemd, outbound HTTPS, the dedicated
service account, and the `cloud-platform` OAuth scope. The scope permits use of
the account's IAM grants; it does not grant permissions itself. Confirm you can
SSH using the approved access path. Only this control VM remains on when idle.

For GitHub, install an organization-owned GitHub App with access to Vulkan-Hpp.
Repository-scoped runner administration requires Administration read/write and
Metadata read access. Confirm the App can create a repository scale set before
opening the pilot to jobs. Record its client ID, installation ID, and private key.
Use this scaler's `SCALER_APP_CLIENT_ID` field, not an ARC Helm configuration.
Alternatively, use a short-lived repository-authorized PAT for the initial trial;
a classic PAT for this repository-scoped configuration needs `repo` access and
the user's permission to administer repository runners. Respect Khronos SSO policy.

Keep these credentials on the control host. They are not workflow secrets and
must not be baked into worker images. See GitHub's
[runner authentication guidance](https://docs.github.com/en/actions/how-tos/manage-runners/use-actions-runner-controller/authenticate-to-the-api)
for the repository/organization permission distinction.

## 3. Create two CPU-only instance templates

On an administrator workstation with `gcloud`, set the actual values. All shell
blocks in this section use the same terminal session. The examples use external
IPs for outbound connectivity; adapt to the project's private-network policy if
needed. Do not add permissive inbound firewall rules.

```bash
set -euo pipefail
export PILOT_GCP_PROJECT='REPLACE_WITH_EXISTING_PROJECT_ID'
export PILOT_IMAGE_PROJECT='REPLACE_WITH_EXISTING_IMAGE_PROJECT_ID'
export PILOT_GCP_ZONE='us-central1-a'
export PILOT_SUBNET='REPLACE_WITH_SUBNET_SELF_LINK'
export PILOT_LINUX_FAMILY='vulkan-hpp-linux-cpu'
export PILOT_WINDOWS_FAMILY='vulkan-hpp-windows-cpu'

PILOT_LINUX_IMAGE=$(gcloud compute images describe-from-family "$PILOT_LINUX_FAMILY" \
  --project="$PILOT_IMAGE_PROJECT" --format='value(name)')
PILOT_WINDOWS_IMAGE=$(gcloud compute images describe-from-family "$PILOT_WINDOWS_FAMILY" \
  --project="$PILOT_IMAGE_PROJECT" --format='value(name)')
export PILOT_LINUX_IMAGE PILOT_WINDOWS_IMAGE
```

Record these concrete image names with the pilot results. Each template pins the
resolved image; moving the image family later does not refresh the template.
Use a new template version when changing an image, and update the service flag.

```bash
gcloud compute instance-templates create vulkan-hpp-pilot-linux-v1 \
  --project="$PILOT_GCP_PROJECT" --machine-type=e2-standard-4 \
  --image="$PILOT_LINUX_IMAGE" --image-project="$PILOT_IMAGE_PROJECT" \
  --boot-disk-size=100GB --boot-disk-type=pd-balanced \
  --subnet="$PILOT_SUBNET" --no-service-account --no-scopes \
  --provisioning-model=STANDARD \
  --labels=purpose=vulkan-hpp-pilot,platform=linux

gcloud compute instance-templates create vulkan-hpp-pilot-windows-v1 \
  --project="$PILOT_GCP_PROJECT" --machine-type=e2-standard-4 \
  --image="$PILOT_WINDOWS_IMAGE" --image-project="$PILOT_IMAGE_PROJECT" \
  --boot-disk-size=100GB --boot-disk-type=pd-balanced \
  --subnet="$PILOT_SUBNET" --no-service-account --no-scopes \
  --provisioning-model=STANDARD \
  --labels=purpose=vulkan-hpp-pilot,platform=windows
```

Increase disk sizes if an image's source disk is larger than 100 GB. Confirm boot
disks are auto-deleted, the templates contain no accelerators, and the images
contain no competing startup registration task. The scaler injects `startup-script`
on Linux or `windows-startup-script-ps1` on Windows, plus `jit-config` and
`expect-gpu=false`. Do not configure a second runner launcher in the template.
Use global instance templates: the current scaler constructs a global template URL.

See Google's [instance template CLI reference](https://docs.cloud.google.com/sdk/gcloud/reference/compute/instance-templates/create).

## 4. Build and install the scaler

Build the public source on a machine with Go 1.25.3 or newer. This draft uses a
fixed source revision so results can be reproduced; agree any newer reviewed
revision with Janne before the trial.

```bash
set -euo pipefail
git clone https://github.com/shader-slang/slang.git slang-scaler-pilot
cd slang-scaler-pilot
git checkout f2b163c08253d6e51a5086fb13d16c1b9c0537c6
cd extras/scaler
GOOS=linux GOARCH=amd64 go build -o scaler-linux ./cmd/scaler
```

Transfer `scaler-linux` and the supplied
[`scaler-vulkan-hpp@.service`](examples/scaler-vulkan-hpp@.service) to the control
host as `/tmp/scaler-linux` and `/tmp/scaler-vulkan-hpp@.service`. The companion
files belong to this draft and must be copied separately if using the fixed
source revision above. On the control host:

```bash
sudo useradd --system --shell /usr/sbin/nologin scaler
sudo install -d -o root -g root -m 0755 /opt/scaler
sudo install -o root -g root -m 0755 /tmp/scaler-linux /opt/scaler/scaler
sudo install -o root -g root -m 0644 /tmp/scaler-vulkan-hpp@.service \
  /etc/systemd/system/scaler-vulkan-hpp@.service
sudo install -o root -g root -m 0600 /dev/null /opt/scaler/vulkan-hpp-pilot.env
sudoedit /opt/scaler/vulkan-hpp-pilot.env
```

If the dedicated `scaler` user already exists, skip `useradd`. Fill the root-only
environment file with the actual project, zone, and one authentication method:

```text
PILOT_GCP_PROJECT=REPLACE_WITH_EXISTING_PROJECT_ID
PILOT_GCP_ZONE=us-central1-a
SCALER_APP_CLIENT_ID=REPLACE_WITH_CLIENT_ID
SCALER_APP_INSTALLATION_ID=REPLACE_WITH_INSTALLATION_ID
SCALER_APP_PRIVATE_KEY="-----BEGIN RSA PRIVATE KEY-----
REPLACE_WITH_ACTUAL_PEM_CONTENTS
-----END RSA PRIVATE KEY-----"
```

Preserve the actual PEM header/footer and real newlines inside the quoted value;
the scaler expects PEM contents, not a key filename or literal backslash-n text.
For PAT authentication replace all three App variables with `SCALER_TOKEN=...`.
Systemd reads the root-only file before launching the unprivileged scaler.

Do not run the existing Slang `deploy/setup-scaler-host.sh` or install its automatic
updater unchanged: the setup targets Slang resources and the updater follows
Slang's release workflow. This pilot uses a manually installed, pinned binary.

```bash
sudo systemctl daemon-reload
sudo systemctl start scaler-vulkan-hpp@linux scaler-vulkan-hpp@windows
sudo journalctl -u scaler-vulkan-hpp@linux -u scaler-vulkan-hpp@windows -n 100
```

Expect two scale sets and no worker VMs while idle. Investigate authentication
or permission errors before dispatching jobs. Do not enable automatic host-boot
startup until you intend the pilot to resume after reboots.

Each service has its own scale-set name, routing label, template, and VM prefix.
Keep `vkhpp-linux` and `vkhpp-windows` exclusive to these services: cleanup discovers
VMs by prefix. Run only one scaler process for each pool. The two per-process
limits total two workers during ordinary operation; after a control-process
crash/restart, inspect existing workers before resuming because process-local
tracking is not a project-wide spending or capacity limit.

## 5. Add the Vulkan-Hpp pilot workflow

Have the maintainer review and copy
[`vulkan-hpp-pilot.yml`](examples/vulkan-hpp-pilot.yml) into Vulkan-Hpp as
`.github/workflows/scaler-pilot.yml`. Merge that workflow to `main` so GitHub can
offer manual dispatch. It has no PR trigger and accepts no arbitrary ref input;
both the workflow and checked-out code use the reviewed main branch.

The Linux and Windows jobs target `[self-hosted, vulkan-hpp-pilot-linux]` and
`[self-hosted, vulkan-hpp-pilot-windows]`, respectively. These match the service's
explicit labels. Do not use generic `build`, `Linux`, or `Windows` labels alone.
Check out the repository with recursive submodules; the presets rely on them.

The commands are adapted from Vulkan-Hpp's existing CI, with bounded build/CTest
parallelism and reduced coverage. The workflow keeps existing action versions
for checkout and MSVC setup; apply Khronos's action-pinning policy before merging.
Do not change existing required checks or replace `.github/workflows/build.yml`.

After merging, dispatch from the Actions page with `main` selected, or run:

```bash
gh workflow run scaler-pilot.yml --repo KhronosGroup/Vulkan-Hpp --ref main
```

Confirm that the Linux compiler selected by CMake is the agreed GCC version and
that Windows finds VS 2022. The generator requires C++20 even though some header
tests use older language standards. No Vulkan SDK/runtime is required by the
reduced dynamic-dispatch preset compilation checks. If samples are later added,
extend the images for their loader, window-system, and build dependencies first.

Public fork PRs remain on existing CI during this trial. Before routing PR code
to these pools, agree the maintainer approval gate and worker trust policy. A
private routing label is not an authorization mechanism. Do not add
`pull_request_target` with a checkout of unreviewed PR code.

## 6. Validate the complete lifecycle

For the first dispatch, follow both service journals and the workflow:

1. A job queues against the intended scale set; the scaler creates a worker in
   the pilot project with the matching exclusive prefix.
2. The worker boots, reads the JIT config, and starts exactly one job. Confirm
   `runner.name`/the job's hostname corresponds to that GCE VM.
3. Header generation and CTest compilation checks finish. Workflow artifacts
   retain the final CTest diagnostics before the VM disappears.
4. The worker shuts down and the scaler deletes the VM and auto-delete boot
   disk. Confirm runner registration cleanup, not only a green build.
5. With no queued work, both pools return to zero workers. The control VM remains.

Dispatch the workflow twice close together to exercise a small burst. With the
supplied configuration, each OS should run one job and queue further demand;
watch the actual instance inventory to verify the cap. Test job cancellation
and a normal build failure, then confirm both clean up. Finally, test a graceful
drain during a running job using the procedure below and confirm it finishes.

Record at least ten job executions per OS, including the burst/cancellation
cases. A practical initial success criterion is no unexplained runner losses,
no leftover VMs/disks/registrations after the cleanup interval, and enough logs
to diagnose failures. The periodic cleanup interval defaults to two minutes;
allow a few minutes for API operations, then investigate leftovers.

For each run record the source SHA, scaler SHA, image names, template version,
compiler/CMake versions, queued/start/completion times, VM create/delete times,
result, and infrastructure retries. Compare matching workloads on GitHub-hosted
runners before drawing cost or performance conclusions. Include the control VM,
Windows licensing, disks, network/IP charges, and failed attempts in pilot cost.

## 7. Diagnose problems and stop safely

| Symptom                       | First checks                                                                                |
| ----------------------------- | ------------------------------------------------------------------------------------------- |
| Queued, no create attempt     | Scale-set registration, exact routing labels, repo/App permissions, service logs            |
| VM creation denied            | Control identity IAM, image/subnet access, CPU/disk/IP quota, selected zone                 |
| VM boots but never runs a job | GCE guest agent, startup log, runner install/ownership, outbound access, competing launcher |
| Build fails immediately       | CMake version, recursive submodules, compiler selected, Windows Git Bash/MSVC availability  |
| Windows runner dies early     | Startup log, available memory/pagefile, runner download/update errors                       |
| Green job leaves resources    | Completion event, delete permissions, prefix/zone configuration, boot disk auto-delete      |

Linux startup logs live in the runner directory's `startup.log`; Windows uses
`C:\actions-runner\startup.log`. Inspect serial-console output if the worker cannot
be reached. Collect logs before deletion when diagnosing an early boot failure.
Do not share `jit-config` metadata or credentials in troubleshooting output.

**Drain before stopping, replacing the binary, or rebooting the host.** A normal
SIGTERM outside drain mode deletes tracked VMs and can interrupt active jobs.

```bash
sudo systemctl reload scaler-vulkan-hpp@linux scaler-vulkan-hpp@windows
sudo journalctl -u scaler-vulkan-hpp@linux -u scaler-vulkan-hpp@windows -f
```

Wait for the drain-completion messages and verify both units are inactive and
all worker jobs have finished. The supplied unit uses `Restart=on-failure`, so a
successful drain exits without immediately recreating the listener. Never use
`systemctl stop` to force a still-active drain merely to save time.

For an update, replace `/opt/scaler/scaler` only after both services have drained,
then explicitly `systemctl start` both units. Keep the previous binary for rollback.

To end the pilot, first disable/remove the pilot workflow so no new jobs queue,
drain both pools, and inspect the project's VM/disk inventory and GitHub runner
settings. Graceful drain preserves scale sets for reuse; after all jobs finish,
remove the pilot scale sets through GitHub runner administration. Remove the two
pilot templates and the control host if no longer needed; retain the existing
image families/project unless their owner separately decides otherwise. Revoke
pilot credentials and verify billing has returned to the expected baseline.

## 8. Decide what follows

After the trial, James and the maintainer should decide whether to expand the
matrix, add samples or PR gating, increase pool limits, and compare the same
workload with ARC. Broader KhronosGroup adoption can initially use separate
repository/pool services; shared organization pools require a separate access,
accounting, and capacity decision.

Use every manual substitution or image/startup incompatibility discovered here
as input to standalone-project extraction. A new cloud-provider API or a full
configuration framework is not a prerequisite for this GCP pilot.

## Source baseline

- [Vulkan-Hpp build workflow](https://github.com/KhronosGroup/Vulkan-Hpp/blob/2d6804b76eefce6039e6afb8f9da7bba0f499043/.github/workflows/build.yml),
  [presets](https://github.com/KhronosGroup/Vulkan-Hpp/blob/2d6804b76eefce6039e6afb8f9da7bba0f499043/CMakePresets.json),
  and [CTest construction](https://github.com/KhronosGroup/Vulkan-Hpp/blob/2d6804b76eefce6039e6afb8f9da7bba0f499043/CMakeLists.txt).
- [Scaler implementation](https://github.com/shader-slang/slang/tree/f2b163c08253d6e51a5086fb13d16c1b9c0537c6/extras/scaler),
  including embedded startup scripts and shutdown behavior.
- [GitHub Scale Set Client](https://github.com/actions/scaleset) for the underlying
  demand/JIT lifecycle. This is a draft deployment recipe; GCP resource names,
  permissions, image compatibility, and end-to-end results must be confirmed by
  the pilot owners before adoption.
