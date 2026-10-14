#ifndef TRACEWEAVE_FSDB_READ_LIFECYCLE_H
#define TRACEWEAVE_FSDB_READ_LIFECYCLE_H

#include <algorithm>
#include <vector>

/* Include after ffrAPI.h. A selection is distinct from resident value data:
 * UnloadSignals does not clear the selected IDs. Own each ID before Add/seek
 * so partial native failures can still be cleaned up, then reset selection.
 * Resident groups borrow this lifetime and must never create another owner.
 */
template <typename Reader>
class FsdbSignalSelection {
    Reader *reader_;
    std::vector<fsdbVarIdcode> ids_;
    bool active_, cleanup_ok_;
    FsdbSignalSelection(const FsdbSignalSelection&) = delete;
    FsdbSignalSelection& operator=(const FsdbSignalSelection&) = delete;
public:
    explicit FsdbSignalSelection(Reader *reader)
        : reader_(reader), active_(false), cleanup_ok_(true) {}
    ~FsdbSignalSelection() { Finish(); }

    int Begin() {
        active_ = true;  // A failed reset may have partially changed state.
        try {
            if (reader_->ffrResetSignalList() == FSDB_RC_SUCCESS) return 0;
        } catch (...) {}
        cleanup_ok_ = false;
        return -6;
    }

    int Add(fsdbVarIdcode id) {
        if (std::find(ids_.begin(), ids_.end(), id) != ids_.end()) return 0;
        ids_.push_back(id);
        return reader_->ffrAddToSignalList(id) == FSDB_RC_SUCCESS ? 0 : -3;
    }

    int Load() { return reader_->ffrLoadSignals() == FSDB_RC_SUCCESS ? 0 : -3; }

    const std::vector<fsdbVarIdcode>& Ids() const { return ids_; }

    void Release() { active_ = false; ids_.clear(); }

    bool Finish() {
        if (!active_) return cleanup_ok_;
        active_ = false;
        for (fsdbVarIdcode id : ids_) {
            try {
                if (reader_->ffrUnloadSignals(id) != FSDB_RC_SUCCESS)
                    cleanup_ok_ = false;
            } catch (...) { cleanup_ok_ = false; }
        }
        ids_.clear();
        try {
            if (reader_->ffrResetSignalList() != FSDB_RC_SUCCESS)
                cleanup_ok_ = false;
        } catch (...) { cleanup_ok_ = false; }
        return cleanup_ok_;
    }
};

#endif
