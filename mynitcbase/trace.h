#ifndef TRACE_H
#define TRACE_H

#include <iostream>
#include <map>
#include <string>
#include <vector>

using namespace std;

// ---------- NODE ----------
struct TraceNode {
    string name;
    int count = 0;
    map<string, TraceNode*> children;
};

// ---------- GLOBAL STATE ----------
extern vector<string> callStack;
extern TraceNode* root;

// ---------- SAFE DELETE TREE ----------
inline void freeTree(TraceNode* node) {
    if (!node) return;
    for (auto &child : node->children) {
        freeTree(child.second);
    }
    delete node;
}

// ---------- INIT PER COMMAND ----------
inline void TRACE_START(string command) {
    if (root) {
        freeTree(root);   // ✅ proper cleanup
    }
    root = new TraceNode();
    root->name = command;
    callStack.clear();
}

// ---------- ENTER FUNCTION ----------
inline void TRACE_ENTER(string module, string func) {
    // ✅ SAFETY: ensure root exists
    if (!root) {
        root = new TraceNode();
        root->name = "UNKNOWN";
    }

    string full = module + "::" + func;
    callStack.push_back(full);

    TraceNode* curr = root;

    for (auto &s : callStack) {
        if (curr->children.count(s) == 0) {
            curr->children[s] = new TraceNode();
            curr->children[s]->name = s;
        }
        curr = curr->children[s];
    }

    curr->count++;
}

// ---------- EXIT FUNCTION ----------
inline void TRACE_EXIT() {
    if (!callStack.empty()) {
        callStack.pop_back();   // ✅ safe pop
    }
}

// ---------- PRINT TREE ----------
inline void printTree(TraceNode* node, int depth = 0) {
    if (!node) return;

    if (depth == 0) {
        cout << "\n=== Command: " << node->name << " ===\n";
    } else {
        for (int i = 0; i < depth - 1; i++) cout << " │   ";
        cout << " ├── " << node->name << " (x" << node->count << ")\n";
    }

    for (auto &child : node->children) {
        printTree(child.second, depth + 1);
    }
}

// ---------- END COMMAND ----------
inline void TRACE_END() {
    if (!root) return;   // ✅ safety
    printTree(root);
}

#endif