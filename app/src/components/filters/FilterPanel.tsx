import {
  CRIME_CATEGORIES,
  type CategoryId,
  type FilterState,
} from "../../lib/types";

const DATE_PRESETS = [
  { id: "7d", label: "7 Days" },
  { id: "30d", label: "30 Days" },
  { id: "6mo", label: "6 Months" },
  { id: "1yr", label: "1 Year" },
];

const SEVERITY_LEVELS = [
  { value: 1, label: "All" },
  { value: 2, label: "Low+" },
  { value: 3, label: "Moderate+" },
  { value: 4, label: "High+" },
  { value: 5, label: "Critical" },
];

interface FilterPanelProps {
  filters: FilterState;
  onToggleCategory: (id: CategoryId) => void;
  onToggleSubcategory: (id: string) => void;
  onSetSeverityMin: (value: number) => void;
  onSetDatePreset: (preset: string | null) => void;
  onSetArrestFilter: (value: boolean | null) => void;
  onClearAll: () => void;
  activeFilterCount: number;
}

export default function FilterPanel({
  filters,
  onToggleCategory,
  onToggleSubcategory,
  onSetSeverityMin,
  onSetDatePreset,
  onSetArrestFilter,
  onClearAll,
  activeFilterCount,
}: FilterPanelProps) {
  return (
    <div className="flex h-full flex-col overflow-y-auto bg-white">
      {/* Header */}
      <div className="flex items-center justify-between border-b border-gray-200 px-4 py-3">
        <h2 className="text-lg font-semibold text-gray-900">Filters</h2>
        {activeFilterCount > 0 && (
          <button
            onClick={onClearAll}
            className="text-sm text-blue-600 hover:text-blue-800"
          >
            Clear all ({activeFilterCount})
          </button>
        )}
      </div>

      {/* Crime Type Section */}
      <div className="border-b border-gray-200 px-4 py-3">
        <h3 className="mb-2 text-sm font-medium text-gray-700">Crime Type</h3>
        {(Object.keys(CRIME_CATEGORIES) as CategoryId[]).map((catId) => {
          const cat = CRIME_CATEGORIES[catId];
          const isExpanded = filters.categories.includes(catId);

          return (
            <div key={catId} className="mb-1">
              <button
                onClick={() => onToggleCategory(catId)}
                className={`flex w-full items-center gap-2 rounded px-2 py-1.5 text-left text-sm transition-colors ${
                  isExpanded
                    ? "bg-gray-100 font-medium text-gray-900"
                    : "text-gray-700 hover:bg-gray-50"
                }`}
              >
                <span
                  className="h-3 w-3 rounded-full"
                  style={{ backgroundColor: cat.color }}
                />
                {cat.label}
              </button>

              {isExpanded && (
                <div className="ml-6 mt-1 space-y-0.5">
                  {cat.subcategories.map((sub) => {
                    const isActive = filters.subcategories.includes(sub.id);
                    return (
                      <label
                        key={sub.id}
                        className="flex cursor-pointer items-center gap-2 rounded px-2 py-1 text-xs text-gray-600 hover:bg-gray-50"
                      >
                        <input
                          type="checkbox"
                          checked={isActive}
                          onChange={() => onToggleSubcategory(sub.id)}
                          className="h-3.5 w-3.5 rounded border-gray-300"
                        />
                        {sub.label}
                      </label>
                    );
                  })}
                </div>
              )}
            </div>
          );
        })}
      </div>

      {/* Severity Section */}
      <div className="border-b border-gray-200 px-4 py-3">
        <h3 className="mb-2 text-sm font-medium text-gray-700">
          Minimum Severity
        </h3>
        <div className="flex flex-wrap gap-1.5">
          {SEVERITY_LEVELS.map((level) => (
            <button
              key={level.value}
              onClick={() => onSetSeverityMin(level.value)}
              className={`rounded-full px-3 py-1 text-xs font-medium transition-colors ${
                filters.severityMin === level.value
                  ? "bg-gray-900 text-white"
                  : "bg-gray-100 text-gray-600 hover:bg-gray-200"
              }`}
            >
              {level.label}
            </button>
          ))}
        </div>
      </div>

      {/* Date Range Section */}
      <div className="border-b border-gray-200 px-4 py-3">
        <h3 className="mb-2 text-sm font-medium text-gray-700">Time Range</h3>
        <div className="flex flex-wrap gap-1.5">
          {DATE_PRESETS.map((preset) => (
            <button
              key={preset.id}
              onClick={() =>
                onSetDatePreset(
                  filters.datePreset === preset.id ? null : preset.id,
                )
              }
              className={`rounded-full px-3 py-1 text-xs font-medium transition-colors ${
                filters.datePreset === preset.id
                  ? "bg-gray-900 text-white"
                  : "bg-gray-100 text-gray-600 hover:bg-gray-200"
              }`}
            >
              {preset.label}
            </button>
          ))}
        </div>
      </div>

      {/* Arrest Status Section */}
      <div className="px-4 py-3">
        <h3 className="mb-2 text-sm font-medium text-gray-700">
          Arrest Status
        </h3>
        <div className="flex flex-wrap gap-1.5">
          {[
            { value: null, label: "Any" },
            { value: true, label: "Arrested" },
            { value: false, label: "No Arrest" },
          ].map((option) => (
            <button
              key={String(option.value)}
              onClick={() => onSetArrestFilter(option.value as boolean | null)}
              className={`rounded-full px-3 py-1 text-xs font-medium transition-colors ${
                filters.arrestMade === option.value
                  ? "bg-gray-900 text-white"
                  : "bg-gray-100 text-gray-600 hover:bg-gray-200"
              }`}
            >
              {option.label}
            </button>
          ))}
        </div>
      </div>
    </div>
  );
}
