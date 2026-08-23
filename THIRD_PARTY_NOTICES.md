# Third-party notices

This repository contains a Python runtime and declares third-party dependencies
through its requirements files and SPDX SBOM. Third-party software remains
subject to its own license terms.

Core declared dependency:

- PyYAML 6.0.3 — MIT license.

Optional S7 dependency:

- python-snap7 3.1.0 — consult the package and upstream project for the license
  applicable to the exact distributed artifact.

Additional optional adapter and tooling backends are listed in
`docs/DEPENDENCIES.md`. They are not bundled merely because an adapter can load
them. Deployers are responsible for reviewing and recording the exact packages,
versions and licenses they install.
