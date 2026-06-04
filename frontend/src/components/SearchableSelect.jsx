import React, { useState, useRef, useEffect, useMemo } from "react";

/**
 * Lightweight searchable dropdown (combobox). No dependencies.
 *
 * Props:
 *   value        currently selected option (string), or "" for none
 *   onChange     (option) => void  — called with the chosen option, or "" on clear
 *   options      string[]          — the selectable values
 *   placeholder  text shown when nothing is selected
 *   maxRender    cap on rendered options (default 200) for large lists
 */
export default function SearchableSelect({
  value,
  onChange,
  options,
  placeholder = "Select…",
  maxRender = 200,
}) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const ref = useRef(null);

  useEffect(() => {
    function onDocClick(e) {
      if (ref.current && !ref.current.contains(e.target)) {
        setOpen(false);
        setQuery("");
      }
    }
    document.addEventListener("mousedown", onDocClick);
    return () => document.removeEventListener("mousedown", onDocClick);
  }, []);

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    const list = q ? options.filter((o) => o.toLowerCase().includes(q)) : options;
    return list.slice(0, maxRender);
  }, [options, query, maxRender]);

  function select(opt) {
    onChange(opt);
    setOpen(false);
    setQuery("");
  }

  return (
    <div className="ss" ref={ref}>
      <input
        type="text"
        className="ss-input"
        value={open ? query : value || ""}
        placeholder={placeholder}
        onChange={(e) => {
          setQuery(e.target.value);
          if (!open) setOpen(true);
        }}
        onFocus={() => setOpen(true)}
      />
      {value && !open && (
        <button
          type="button"
          className="ss-clear"
          aria-label="Clear"
          onMouseDown={(e) => {
            e.preventDefault();
            onChange("");
          }}
        >
          ×
        </button>
      )}
      {open && (
        <ul className="ss-list">
          {filtered.length === 0 ? (
            <li className="ss-empty">No matches</li>
          ) : (
            filtered.map((opt) => (
              <li
                key={opt}
                className={"ss-opt" + (opt === value ? " ss-opt-sel" : "")}
                onMouseDown={() => select(opt)}
              >
                {opt}
              </li>
            ))
          )}
          {!query.trim() && options.length > filtered.length && (
            <li className="ss-more">
              Showing {filtered.length} of {options.length} — type to search…
            </li>
          )}
        </ul>
      )}
    </div>
  );
}
