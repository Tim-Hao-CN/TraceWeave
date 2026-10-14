#ifndef TRACEWEAVE_FSDB_EVENT_CURSOR_H
#define TRACEWEAVE_FSDB_EVENT_CURSOR_H

#include <limits>
#include <vector>

/* Include after ffrAPI.h. A resume token describes the NEXT unread record,
 * without rounding raw tags through the public picosecond representation.
 * GotoXTag lands on the last record at a tick; an initial predecessor keeps
 * that marker until advance, avoiding an unnecessary walk across its glitches.
 */
struct FsdbEventPosition {
    fsdbTag64 tag = {};
    unsigned long long ordinal = 0;
    bool last_at_tick = false;

    static bool Equal(const fsdbTag64 &a, const fsdbTag64 &b) {
        return a.H == b.H && a.L == b.L;
    }

    template <typename Handle>
    bool Initialize(Handle hdl, bool last) {
        ordinal = 0;
        last_at_tick = last;
        return hdl->ffrGetXTag(&tag) == FSDB_RC_SUCCESS;
    }

    /* 1 = next record, 0 = EOF, -3 = a position that cannot be resumed. */
    template <typename Handle>
    int Advance(Handle hdl) {
        if (hdl->ffrGotoNextVC() != FSDB_RC_SUCCESS) return 0;
        fsdbTag64 next;
        if (hdl->ffrGetXTag(&next) != FSDB_RC_SUCCESS) return -3;
        if (Equal(tag, next)) {
            if (last_at_tick || ordinal == std::numeric_limits<unsigned long long>::max())
                return -3;
            ++ordinal;
        } else {
            ordinal = 0;
            last_at_tick = false;
        }
        tag = next;
        return 1;
    }

    template <typename Handle>
    bool Restore(Handle hdl) const {
        fsdbTag64 seek_tag = tag;
        // Demand-load the target session first. FirstVC on a newly created,
        // unloaded handle is not a reliable emptiness test (including tick 0).
        if (hdl->ffrGotoXTag(&seek_tag) != FSDB_RC_SUCCESS) return false;
        if (last_at_tick) {
            // Already positioned on the requested last record at this tick.
        } else if (tag.H == 0 && tag.L == 0) {
            if (hdl->ffrGotoTheFirstVC() != FSDB_RC_SUCCESS) return false;
        } else {
            // Seek before the raw tick, then advance to its FIRST record. This
            // avoids traversing the unread tail of a same-tick glitch group.
            seek_tag = tag;  // GotoXTag may have changed its argument.
            if (seek_tag.L == 0) { --seek_tag.H; seek_tag.L = 0xffffffff; }
            else --seek_tag.L;
            if (hdl->ffrGotoXTag(&seek_tag) == FSDB_RC_SUCCESS) {
                if (hdl->ffrGotoNextVC() != FSDB_RC_SUCCESS) return false;
            } else if (hdl->ffrGotoTheFirstVC() != FSDB_RC_SUCCESS) {
                return false;
            }
        }
        fsdbTag64 landed;
        if (hdl->ffrGetXTag(&landed) != FSDB_RC_SUCCESS || !Equal(tag, landed))
            return false;
        if (!last_at_tick) {
            for (unsigned long long i = 0; i < ordinal; ++i) {
                if (hdl->ffrGotoNextVC() != FSDB_RC_SUCCESS ||
                    hdl->ffrGetXTag(&landed) != FSDB_RC_SUCCESS || !Equal(tag, landed))
                    return false;
            }
        }
        return true;
    }
};

/* All persistent handles of this signal must participate. Their saved
 * positions are immutable throughout the operation. A failed free leaves
 * ownership uncertain: make no unload/recreate attempt in that state, and
 * require closing the reader. Nulling before free also prevents double free
 * after an SDK exception that may have occurred after releasing the handle.
 */
template <typename Reader, typename Cursor>
bool FsdbReclaimEventCursors(Reader *reader, fsdbVarIdcode id,
                            const std::vector<Cursor*> &cursors,
                            bool &cleanup_failed) {
    if (cleanup_failed) return false;
    for (Cursor *cursor : cursors) {
        if (!cursor->hdl) continue;
        auto hdl = cursor->hdl;
        cursor->hdl = nullptr;
        try { hdl->ffrFree(); }
        catch (...) { cleanup_failed = true; }
    }
    if (cleanup_failed) return false;
    try {
        if (reader->ffrUnloadSignals(id) != FSDB_RC_SUCCESS) {
            cleanup_failed = true;
            return false;
        }
        for (Cursor *cursor : cursors) {
            if (!cursor->valid) continue;
            cursor->hdl = reader->ffrCreateVCTraverseHandle(id);
            if (!cursor->hdl || !cursor->position.Restore(cursor->hdl)) {
                cleanup_failed = true;
                break;
            }
        }
    } catch (...) { cleanup_failed = true; }
    if (cleanup_failed) {
        // Release successfully recreated handles, including the one whose
        // seek failed. Keep the reader poisoned even if this cleanup succeeds.
        for (Cursor *cursor : cursors) {
            if (!cursor->hdl) continue;
            auto hdl = cursor->hdl;
            cursor->hdl = nullptr;
            try { hdl->ffrFree(); }
            catch (...) {}
        }
    }
    return !cleanup_failed;
}

#endif
