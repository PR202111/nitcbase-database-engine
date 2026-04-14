#ifndef TRACE_H
#define TRACE_H

#include <iostream>
#include <vector>
#include <string>

using namespace std;

// ---------- NODE ----------
struct TraceNode {
    string name;
    int count = 1;
    vector<TraceNode*> children;
};

// ---------- GLOBAL ----------
extern vector<TraceNode*> nodeStack;
extern TraceNode* root;

// ---------- FREE TREE ----------
inline void freeTree(TraceNode* node) {
    if (!node) return;
    for (auto child : node->children)
        freeTree(child);
    delete node;
}

// ---------- START ----------
inline void TRACE_START(string cmd) {
    if (root) freeTree(root);
    root = nullptr;
    nodeStack.clear();
}

// ---------- ENTER ----------
inline void TRACE_ENTER(string module, string func) {
    string full = module + "::" + func;

    TraceNode* newNode = new TraceNode();
    newNode->name = full;

    if (nodeStack.empty()) {
        root = newNode;
    } else {
        TraceNode* parent = nodeStack.back();

        // ✅ merge consecutive duplicates
        if (!parent->children.empty() &&
            parent->children.back()->name == full) {

            parent->children.back()->count++;
            nodeStack.push_back(parent->children.back());
            return;
        }

        parent->children.push_back(newNode);
    }

    nodeStack.push_back(newNode);
}

// ---------- EXIT ----------
inline void TRACE_EXIT() {
    if (!nodeStack.empty())
        nodeStack.pop_back();
}

// ---------- PRINT ----------
inline void printTree(TraceNode* node, int depth = 0) {
    if (!node) return;

    for (int i = 0; i < depth; i++)
        cout << "|   ";

    cout << "|____ " << node->name;

    if (node->count > 1)
        cout << " (x" << node->count << ")";

    cout << "\n";

    for (auto child : node->children)
        printTree(child, depth + 1);
}

// ---------- END ----------
inline void TRACE_END() {
    cout << "\n=== Execution Trace ===\n";
    printTree(root);
}

#endif