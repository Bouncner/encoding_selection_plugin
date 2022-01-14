# Hyrise - Plugin Collection for the "Encoding Selection" Project

![Workflow](https://github.com/Bouncner/hyrise_workload_analysis_plugin/actions/workflows/main.yml/badge.svg)

The purpose of these plugins is to provide a playground for "autonomous" plugins. The plugins support exporting calibration data for model learning, exporting the workload's PQP to CSV, means to execute commands in the database, and more.

The goal is to run on an unmodified Hyrise master. Several previous projects in this direction required a lot of adaptions which were hard to get back into the master.

Please note, that the Hyrise submodule *does not use the master* (currently defaults to the `martin/compression_selection_project` branch), but the goal is to always be executable with Hyrise's master.
For use with the compression selection project, the branch contains a few performance optimizations which might never make their way into the Hyrise master.

## Plugins

Hyrise is a research database that is also regularly used for teaching at the Hasso Plattner Institute.
Thus, we aim to have a clear and concise code base which makes it easy for student to get familiar with.
Research projects such as the encoding selection project often require multiple changes to the database system.
Especially in the early phase of development.
For this reason, Hyrise has a plugin interface that allows users to dynamically load code.

For of the changes done to the Hyrise submodule are planned to get (in some form or another) merged in the master.
Other components do neither have the quality to land in the master, nor are we certain we would do it exactly like that again if we need to (e.g., the PQP export).
These components are implemented as plugins on top of (the slightly modified) Hyrise.

* **Data Characteristics Plugin**: Gathers additional segment information that is currently not stored in Hyrise's `meta_segments` or `meta_segments_accurate` tables (e.g., value switches (relevant for RLE) or string information such as the average string length). This information would ideally be gathered once during the first encoding.
* **Command Executor Plugin**: Hyrise lacks the capability to send arbitrary commands via SQL. E.g., to apply an encoding information passed as a JSON file. The command executor plugin does exactly that in a very hard-coded fashion. We first write the command a meta table and than select from this table. The code that is called for the query checks if the inserted row is a known command and executes it.
* **PQP Export Tables Plugin**: Exports query cache information from the physical query plan (PQP) cache to CSV files. Pretty much optmized for the encoding use case but easy to extend. From an efficiency  point of view, this whole part of exporting to csv, learning, and applying models should be done in C++. But for fast prototyping, having a workload in CSV form and using SciKit/Python for the learning stuff was much easier.
* **Workload Handler Plugin**: Hyrise includes various forms of "workload query sets" that are used for the binary benchmarks. For TPC-H, we generate them according to the specification. For the Join Order Benchmark, we use the provided SQL files. To gather all queries of a workload that Hyrise "already has", we use this plugin to simply provide the queries as a  meta table.
