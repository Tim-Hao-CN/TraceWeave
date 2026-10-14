// Resume positions and sibling ownership without a Verdi SDK or license.
#include <cassert>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

struct fsdbTag64 { unsigned H, L; };
typedef unsigned fsdbVarIdcode;
const int FSDB_RC_SUCCESS = 0;

#include "fsdb_event_cursor.h"

struct Reader;
struct Handle {
    Reader *reader;
    unsigned id;
    bool live = true;
    bool demand_loaded = false;
    size_t index = 0;
    Handle(Reader *r, unsigned i) : reader(r), id(i) {}
    void ffrFree();
    int ffrGetXTag(fsdbTag64 *tag);
    int ffrGotoXTag(fsdbTag64 *tag);
    int ffrGotoTheFirstVC();
    int ffrGotoNextVC();
};

struct Reader {
    // Include raw sub-ps positions, glitches, a sparse gap and 32-bit carry.
    std::vector<fsdbTag64> rows = {{0,0}, {0,0}, {0,1}, {0,9},
        {0,10}, {0,10}, {0,10}, {0,10}, {0,11}, {0,1000000},
        {0,0xffffffff}, {1,0}, {1,0}, {1,7}};
    std::vector<std::unique_ptr<Handle>> handles;
    std::string failure;
    unsigned frees = 0, unloads = 0, creates = 0, gets = 0, seeks = 0;

    Handle *ffrCreateVCTraverseHandle(unsigned id) {
        ++creates;
        if (failure == "throw_create") throw std::runtime_error("create");
        if (failure == "create" || (failure == "second_create" && creates == 2))
            return nullptr;
        handles.emplace_back(new Handle(this, id));
        return handles.back().get();
    }
    int ffrUnloadSignals(unsigned id) {
        for (const auto &handle : handles) assert(handle->id != id || !handle->live);
        ++unloads;
        if (failure == "throw_unload") throw std::runtime_error("unload");
        return failure == "unload" ? 1 : 0;
    }
    unsigned Live(unsigned id) const {
        unsigned live = 0;
        for (const auto &handle : handles) live += handle->live && handle->id == id;
        return live;
    }
};

void Handle::ffrFree() {
    assert(live);
    ++reader->frees;
    if (reader->failure == "throw_free_before") throw std::runtime_error("free");
    live = false;
    if (reader->failure == "throw_free") throw std::runtime_error("free");
}

int Handle::ffrGetXTag(fsdbTag64 *tag) {
    assert(live);
    if (!demand_loaded) return 1;
    ++reader->gets;
    if (reader->failure == "throw_get") throw std::runtime_error("get");
    if (reader->failure == "get") return 1;
    *tag = reader->rows[index];
    return 0;
}

int Handle::ffrGotoXTag(fsdbTag64 *tag) {
    assert(live);
    ++reader->seeks;
    if (reader->failure == "throw_seek") throw std::runtime_error("seek");
    if (reader->failure == "seek") { *tag = {9,9}; return 1; }
    size_t next = 0;
    while (next < reader->rows.size()) {
        const auto &row = reader->rows[next];
        if (row.H > tag->H || (row.H == tag->H && row.L > tag->L)) break;
        ++next;
    }
    if (!next) { *tag = {9,9}; return 1; }
    demand_loaded = true;
    index = next - 1;
    *tag = reader->rows[index]; // The real API may mutate the requested tag.
    if (reader->failure == "wrong_tick") { index = 0; *tag = reader->rows[0]; }
    return 0;
}

int Handle::ffrGotoTheFirstVC() {
    assert(live);
    if (!demand_loaded || reader->failure == "first") return 1;
    index = 0;
    return 0;
}

int Handle::ffrGotoNextVC() {
    assert(live);
    if (!demand_loaded) return 1;
    if (reader->failure == "throw_next") throw std::runtime_error("next");
    if (reader->failure == "next" || index + 1 == reader->rows.size()) return 1;
    ++index;
    return 0;
}

struct Cursor {
    Handle *hdl;
    bool valid;
    FsdbEventPosition position;
};

static Cursor At(Reader &reader, size_t index, bool last = false, unsigned id = 1) {
    Cursor cursor = {reader.ffrCreateVCTraverseHandle(id), true, {}};
    fsdbTag64 first = reader.rows.front();
    assert(cursor.hdl->ffrGotoXTag(&first) == 0);
    assert(cursor.hdl->ffrGotoTheFirstVC() == 0);
    assert(cursor.position.Initialize(cursor.hdl, false));
    if (last) {
        fsdbTag64 tag = reader.rows[index];
        assert(cursor.hdl->ffrGotoXTag(&tag) == 0);
        assert(cursor.hdl->index == index);
        assert(cursor.position.Initialize(cursor.hdl, true));
    } else {
        for (size_t i = 0; i < index; ++i) assert(cursor.position.Advance(cursor.hdl) == 1);
    }
    return cursor;
}

int main(int argc, char **argv) {
    assert(argc == 2);
    const std::string scenario = argv[1];
    Reader reader;
    if (scenario == "positions" || scenario == "nonzero_origin") {
        if (scenario == "nonzero_origin") reader.rows.erase(reader.rows.begin(), reader.rows.begin() + 4);
        for (size_t i = 0; i < reader.rows.size(); ++i) {
            for (bool last : {false, true}) {
                if (last && i + 1 < reader.rows.size() &&
                    FsdbEventPosition::Equal(reader.rows[i], reader.rows[i+1])) continue;
                Cursor c = At(reader, i, last);
                bool failed = false;
                const fsdbTag64 before = c.position.tag;
                const auto ordinal = c.position.ordinal;
                for (unsigned repeat = 0; repeat < 3; ++repeat) {
                    assert(FsdbReclaimEventCursors(&reader, 1, std::vector<Cursor*>{&c}, failed));
                    assert(!failed && c.hdl->index == i);
                    assert(FsdbEventPosition::Equal(before, c.position.tag));
                    assert(c.position.ordinal == ordinal && c.position.last_at_tick == last);
                }
                if (i + 1 < reader.rows.size()) {
                    assert(c.position.Advance(c.hdl) == 1);
                    assert(c.hdl->index == i + 1);
                    if (!FsdbEventPosition::Equal(before, c.position.tag))
                        assert(c.position.ordinal == 0 && !c.position.last_at_tick);
                } else assert(c.position.Advance(c.hdl) == 0);
                c.hdl->ffrFree();
            }
        }
        return 0;
    }
    Cursor a = At(reader, 5), b = At(reader, 7, true), eof = At(reader, 13);
    Cursor unrelated = At(reader, 8, false, 2);
    eof.valid = false;
    const auto unrelated_handle = unrelated.hdl;
    const unsigned old_frees = reader.frees;
    reader.creates = 0;
    reader.failure = scenario == "siblings" ? "" : scenario;
    bool failed = false;
    const bool ok = FsdbReclaimEventCursors(&reader, 1, std::vector<Cursor*>{&a,&b,&eof}, failed);
    if (scenario == "siblings") {
        assert(ok && !failed && reader.creates == 2 && reader.unloads == 1);
        assert(reader.frees == old_frees + 3);
        assert(a.hdl->index == 5 && b.hdl->index == 7 && !eof.hdl);
        assert(a.position.ordinal == 1 && b.position.last_at_tick);
        assert(reader.Live(1) == 2);
        a.hdl->ffrFree(); b.hdl->ffrFree();
    } else {
        assert(!ok && failed);
        assert(!a.hdl && !b.hdl && !eof.hdl);
        assert(reader.Live(1) == (scenario == "throw_free_before" ? 3U : 0U));
        assert(reader.unloads == ((scenario == "throw_free" || scenario == "throw_free_before") ? 0U : 1U));
        unsigned frees = reader.frees, unloads = reader.unloads, creates = reader.creates;
        assert(!FsdbReclaimEventCursors(&reader, 1, std::vector<Cursor*>{&a,&b}, failed));
        assert(reader.frees == frees && reader.unloads == unloads && reader.creates == creates);
    }
    assert(unrelated.hdl == unrelated_handle && unrelated.hdl->index == 8 && reader.Live(2) == 1);
    reader.failure.clear();
    unrelated.hdl->ffrFree();
}
