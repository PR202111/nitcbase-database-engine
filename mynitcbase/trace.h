#ifndef TRACE_H
#define TRACE_H

#include <iostream>
#include <vector>
#include <string>

using namespace std;

// ===== GLOBAL STATE =====
extern vector<string> traceLog;
extern vector<bool> branchActive; // track vertical lines

// ===== START =====
inline void TRACE_START(string command) {
    traceLog.clear();
    branchActive.clear();

    cout << "\n=== Command: " << command << " ===\n";
}

// ===== BUILD PREFIX =====
inline string buildPrefix() {
    string prefix = "";

    for (size_t i = 0; i < branchActive.size(); i++) {
        if (branchActive[i])
            prefix += "|   ";
        else
            prefix += "    ";
    }

    return prefix;
}

// ===== ENTER =====
inline void TRACE_ENTER(const char* module, const char* func) {
    string prefix = buildPrefix();

    if (!branchActive.empty()) {
        prefix += "|____ ";
    }

    traceLog.push_back(prefix + string(module) + "::" + string(func));

    branchActive.push_back(true);
}

// ===== EXIT =====
inline void TRACE_EXIT() {
    if (!branchActive.empty()) {
        branchActive.pop_back();
    }
}

// ===== END =====
inline void TRACE_END() {
    cout << "\n=== Execution Trace ===\n";
    for (auto &line : traceLog) {
        cout << line << endl;
    }
    cout << endl;
}

#endif