package de.hpi.ddm.utils;

import java.util.Comparator;

import de.hpi.ddm.structures.WorkItem;

public class WorkItemComparator implements Comparator<WorkItem> {
    @Override
    public int compare(WorkItem lhs, WorkItem rhs) {
        // First use uncracked ones.
        // Then use the number of cracking workers.
        if (!lhs.isCracked() && !rhs.isCracked()) {
            final int crackingWorkersDiff = lhs.getWorkersCracking().size() - rhs.getWorkersCracking().size();
            if (crackingWorkersDiff != 0) {
                return crackingWorkersDiff;
            }
            // If the same number of cracking workers, use ID for order.
            return lhs.getPasswordEntry().getId() - rhs.getPasswordEntry().getId();
        } else if (lhs.isCracked() && !rhs.isCracked()) {
            return 1;
        } else if (!lhs.isCracked() && rhs.isCracked()) {
            return -1;
        }
        // Both are cracked, use the ID for sortation.
        return lhs.getPasswordEntry().getId() - rhs.getPasswordEntry().getId();
    }
}
