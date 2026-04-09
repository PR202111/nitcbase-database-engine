# 🚀 NITCbase

A modular relational database engine built in C++, designed for learning
internal DBMS architecture --- with **function-level tracing** for deep
understanding.

------------------------------------------------------------------------

## 📌 Overview

**NITCbase** simulates core database components like:

-   Storage Manager
-   Buffer Manager
-   Cache Layer
-   B+ Tree Indexing
-   Query Processing (Algebra + Frontend)

🔍 What makes this special: \> A **custom tracing system** that shows
*exact function-level execution flow* --- ideal for learning and
debugging.

------------------------------------------------------------------------

## 🧠 Features

### ✅ Core DB Features

-   Create / Open / Delete Relations
-   Insert / Search Records
-   Block-level storage handling
-   Attribute & Relation catalogs
-   B+ Tree indexing
-   Query execution (Select, Project, Join)

------------------------------------------------------------------------

### 🔍 Function Call Tracing (Additional Feature ⭐)

-   Tracks **every function call**
-   Builds a **hierarchical call tree**
-   Counts repeated calls `(xN)`
-   Helps visualize execution flow and module interaction

------------------------------------------------------------------------

## 🏗️ Project Structure

. ├── FrontendInterface/ ├── Frontend/ ├── Algebra/ ├── Schema/ ├──
BlockAccess/ ├── BPlusTree/ ├── Cache/ ├── Buffer/ ├── Disk_Class/ ├──
define/ ├── trace.h ├── trace.cpp ├── main.cpp ├── Makefile

------------------------------------------------------------------------

## ⚙️ Build Modes

### 🟢 Normal Mode

make\
./nitcbase

------------------------------------------------------------------------

### 🐞 Debug Mode

make mode=debug\
./nitcbase-debug

------------------------------------------------------------------------

### 🔍 Trace Mode

make mode=trace\
./nitcbase-trace


------------------------------------------------------------------------

## 🛠️ Tracing Usage

Add this to any function:

TRACE_FUNC("ModuleName");

------------------------------------------------------------------------

## 📚 Learning Value

-   Understand DBMS internals
-   See real execution flow
-   Learn system-level design

