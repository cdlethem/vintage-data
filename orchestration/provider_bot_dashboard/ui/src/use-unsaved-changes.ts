import { useEffect } from "react";

// Keep drafts in memory. In particular, never persist provider credentials in browser storage.
export function useUnsavedChanges(dirty: boolean, onDirtyChange?: (dirty: boolean) => void) {
  useEffect(() => {
    onDirtyChange?.(dirty);
    return () => onDirtyChange?.(false);
  }, [dirty, onDirtyChange]);
  useEffect(() => {
    if (!dirty) return;
    const beforeUnload = (event: BeforeUnloadEvent) => {
      event.preventDefault();
      event.returnValue = "";
    };
    window.addEventListener("beforeunload", beforeUnload);
    return () => window.removeEventListener("beforeunload", beforeUnload);
  }, [dirty]);
}
