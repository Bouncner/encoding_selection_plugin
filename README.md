# Hyrise - Plugin Collection for the "Compression Selection" Project

![Workflow](https://github.com/Bouncner/hyrise_workload_analysis_plugin/actions/workflows/main.yml/badge.svg)

The purpose of these plugins is to provide a playground for "autonomous" plugins. The plugins support exporting calibration data for model learning, exporting the workload's PQP to CSV, means to execute commands in the database, and more.

The goal is to run on an unmodified Hyrise master. Several previous projects in this direction required a lot of adaptions which were hard to get back into the master.

Please note, that the Hyrise submodule *does not use the master* (currently defaults to the `martin/compression_selection_project` branch), but the goal is to always be executable with Hyrise's master.
For use with the compression selection project, the branch contains a few performance optimizations which might never make their way into the Hyrise master.