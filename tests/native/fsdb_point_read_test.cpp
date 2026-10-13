// Exercise production selection/point lifetimes without a Verdi installation.
#include <cassert>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

struct fsdbTag64 { unsigned H, L; };
typedef fsdbTag64 fsdbXTag;
typedef unsigned fsdbVarIdcode;
const int FSDB_RC_SUCCESS = 0;

#include "fsdb_point_read.h"

struct Handle {
    std::vector<std::string> *calls;
    std::string *failure;
    bool live = false;
    Handle(std::vector<std::string> *c, std::string *f) : calls(c), failure(f) {}
    void ffrFree() {
        assert(live);
        live = false;
        calls->push_back("free");
        if (*failure == "throw_free") throw std::runtime_error("free");
    }
};

struct Reader {
    fsdbTag64 start = {7, 10}, end = {8, 10};
    std::vector<std::string> calls;
    std::string failure;
    Handle handle = Handle(&calls, &failure);
    std::set<fsdbVarIdcode> selected, resident;
    std::vector<std::set<fsdbVarIdcode>> loads;
    int resets = 0, selection_resets = 0;

    int ffrGetMinFsdbTag64(fsdbTag64 *tag) {
        *tag = start;
        return failure == "bounds" ? 1 : 0;
    }
    int ffrGetMaxFsdbTag64(fsdbTag64 *tag) { *tag = end; return 0; }
    int ffrResetViewWindow(fsdbXTag *a, fsdbXTag *b) {
        calls.push_back("window");
        if (failure == "unsupported") return 1;
        start = *a; end = *b; ++resets;
        // Mutate before failing to model partially applied native operations.
        if (failure == "throw_restore" && resets == 2) throw std::runtime_error("restore");
        return (failure == "set" && resets == 1) ||
               (failure == "restore" && resets == 2) ? 1 : 0;
    }
    int ffrResetSignalList() {
        calls.push_back("reset");
        assert(!handle.live);
        selected.clear();
        ++selection_resets;
        bool initial = selection_resets == 1;
        if (failure == (initial ? "throw_reset_initial" : "throw_reset_cleanup"))
            throw std::runtime_error("reset");
        return failure == (initial ? "reset_initial" : "reset_cleanup") ? 1 : 0;
    }
    int ffrAddToSignalList(fsdbVarIdcode id) {
        calls.push_back("add");
        selected.insert(id);
        // Partial Add can allocate storage before reporting failure.
        if (failure == "add" || failure == "throw_add") resident.insert(id);
        if (failure == "throw_add") throw std::runtime_error("add");
        return failure == "add" ? 1 : 0;
    }
    int ffrLoadSignals() {
        calls.push_back("load");
        resident.insert(selected.begin(), selected.end());
        loads.push_back(selected);
        if (failure == "throw_load") throw std::runtime_error("load");
        return failure == "load" ? 1 : 0;
    }
    Handle *ffrCreateVCTraverseHandle(fsdbVarIdcode id) {
        calls.push_back("create");
        assert(resident.count(id));
        if (failure == "throw_create") throw std::runtime_error("create");
        if (failure == "create") return nullptr;
        handle.live = true;
        return &handle;
    }
    int ffrUnloadSignals(fsdbVarIdcode id) {
        calls.push_back("unload");
        assert(!handle.live);  // Handles must be released before their storage.
        resident.erase(id);
        if (failure == "throw_unload") throw std::runtime_error("unload");
        return failure == "unload" ? 1 : 0;
    }
};

int main(int argc, char **argv) {
    assert(argc == 2);
    Reader reader;
    reader.failure = argv[1];
    const std::string test = reader.failure;
    if (test == "selection_dedup" || test == "selection_transfer") {
        {
            FsdbSignalSelection<Reader> owner(&reader);
            assert(owner.Begin() == 0);
            assert(owner.Add(1) == 0 && owner.Add(2) == 0 && owner.Add(1) == 0);
            assert(owner.Load() == 0);
            assert(owner.Ids() == std::vector<fsdbVarIdcode>({1, 2}));
            assert(reader.selected == std::set<fsdbVarIdcode>({1, 2}));
            if (test == "selection_transfer") owner.Release();
        }
        if (test == "selection_transfer") {
            // Group ownership has moved to the caller; destruction cannot unload it.
            assert(reader.selected == std::set<fsdbVarIdcode>({1, 2}));
            assert(reader.resident == reader.selected);
            assert(reader.calls == std::vector<std::string>({"reset", "add", "add", "load"}));
        } else {
            assert(reader.selected.empty() && reader.resident.empty());
            assert(reader.calls == std::vector<std::string>({"reset", "add", "add", "load", "unload", "unload", "reset"}));
        }
        return 0;
    }
    bool resident = test == "resident";
    bool allow = test != "disabled";
    reader.selected = resident ? std::set<fsdbVarIdcode>({1, 2}) : std::set<fsdbVarIdcode>({99});
    if (resident) reader.resident = reader.selected;
    fsdbTag64 point = {7, 50};
    if (test == "before") point = {6, 50};
    if (test == "after") point = {9, 50};
    bool bypass = resident || !allow || test == "bounds" || test == "before" || test == "after";
    bool reset_failed = test == "reset_initial" || test == "throw_reset_initial";
    bool cleanup_failed = reset_failed || test == "reset_cleanup" || test == "throw_reset_cleanup" ||
        test == "restore" || test == "unload" || test == "throw_restore" ||
        test == "throw_unload" || test == "throw_free";
    bool threw = false;
    try {
        FsdbPointRead<Reader, Handle*> read(&reader);
        int rc = read.Open(1, point, resident, allow);
        bool early = test == "add" || test == "load" || test == "create";
        assert(rc == (reset_failed ? -6 : early ? -3 : 0));
        assert(read.Narrowed() == (!bypass && test != "set" && test != "unsupported"));
        if (test == "throw_body") throw std::runtime_error("body");
        assert(read.Finish() == !cleanup_failed);
        assert(read.Finish() == !cleanup_failed);  // idempotent before destructor
    } catch (const std::runtime_error&) {
        threw = true;
    }
    assert(threw == (test == "throw_add" || test == "throw_load" ||
                     test == "throw_create" || test == "throw_body"));
    assert(reader.start.H == 7 && reader.start.L == 10);
    assert(reader.end.H == 8 && reader.end.L == 10);
    assert(!reader.handle.live);
    if (resident) {
        assert(reader.selected == std::set<fsdbVarIdcode>({1, 2}));
        assert(reader.resident == reader.selected);
    } else {
        assert(reader.selected.empty() && reader.resident.empty());
        for (const auto &loaded : reader.loads) assert(loaded == std::set<fsdbVarIdcode>({1}));
    }

    std::vector<std::string> expected;
    if (!bypass) expected.push_back("window");
    if (test == "set") expected.push_back("window");
    bool added = !reset_failed;
    bool loaded = added && test != "add" && test != "throw_add";
    bool created = loaded && test != "load" && test != "throw_load";
    if (!resident) {
        expected.push_back("reset");
        if (added) expected.push_back("add");
        if (loaded) expected.push_back("load");
    }
    if (created) {
        expected.push_back("create");
        if (test != "create" && test != "throw_create") expected.push_back("free");
    }
    if (!resident) {
        if (added) expected.push_back("unload");
        expected.push_back("reset");
    }
    if (!bypass && test != "set" && test != "unsupported") expected.push_back("window");
    assert(reader.calls == expected);
}
