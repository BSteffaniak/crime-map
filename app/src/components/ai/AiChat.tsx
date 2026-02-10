import { useCallback, useEffect, useRef, useState } from "react";
import { useAiChat, type AiStep, type ChatMessage } from "../../hooks/useAiChat";

const SUGGESTED_QUESTIONS = [
  "What are the safest neighborhoods in Washington, DC?",
  "How has crime changed in Chicago year over year?",
  "Which city has the most violent crime?",
  "What are the most common crime types in San Francisco?",
  "Compare property crime in 2024 vs 2025 for Los Angeles",
];

export default function AiChat() {
  const { messages, loading, ask, clear } = useAiChat();
  const [input, setInput] = useState("");
  const scrollRef = useRef<HTMLDivElement>(null);

  // Auto-scroll to bottom when messages change
  useEffect(() => {
    const el = scrollRef.current;
    if (el) {
      el.scrollTop = el.scrollHeight;
    }
  }, [messages]);

  const handleSubmit = useCallback(
    (e: React.FormEvent) => {
      e.preventDefault();
      const q = input.trim();
      if (!q || loading) return;
      setInput("");
      ask(q);
    },
    [input, loading, ask],
  );

  const handleSuggestion = useCallback(
    (question: string) => {
      if (loading) return;
      ask(question);
    },
    [loading, ask],
  );

  return (
    <div className="flex h-full flex-col bg-white">
      {/* Header */}
      <div className="flex items-center justify-between border-b border-gray-200 px-4 py-3">
        <div>
          <h2 className="text-lg font-semibold text-gray-900">Ask AI</h2>
          <p className="text-xs text-gray-500">
            Ask questions about crime data
          </p>
        </div>
        {messages.length > 0 && (
          <button
            onClick={clear}
            className="text-xs text-gray-400 hover:text-gray-600 transition-colors"
          >
            Clear
          </button>
        )}
      </div>

      {/* Messages */}
      <div ref={scrollRef} className="flex-1 overflow-y-auto px-4 py-3 space-y-4">
        {messages.length === 0 && (
          <div className="space-y-3">
            <p className="text-sm text-gray-500">
              Ask questions about crime patterns, trends, and statistics across US cities. For example:
            </p>
            <div className="space-y-2">
              {SUGGESTED_QUESTIONS.map((q) => (
                <button
                  key={q}
                  onClick={() => handleSuggestion(q)}
                  className="block w-full rounded-lg border border-gray-200 px-3 py-2 text-left text-sm text-gray-700 transition-colors hover:border-blue-300 hover:bg-blue-50"
                >
                  {q}
                </button>
              ))}
            </div>
          </div>
        )}

        {messages.map((msg) => (
          <MessageBubble key={msg.id} message={msg} />
        ))}
      </div>

      {/* Input */}
      <form
        onSubmit={handleSubmit}
        className="border-t border-gray-200 px-4 py-3"
      >
        <div className="flex gap-2">
          <input
            type="text"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            placeholder="Ask about crime data..."
            disabled={loading}
            className="flex-1 rounded-lg border border-gray-300 px-3 py-2 text-sm placeholder-gray-400 focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500 disabled:bg-gray-50 disabled:text-gray-400"
          />
          <button
            type="submit"
            disabled={loading || !input.trim()}
            className="rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white transition-colors hover:bg-blue-700 disabled:bg-gray-300 disabled:cursor-not-allowed"
          >
            {loading ? (
              <span className="inline-block h-4 w-4 animate-spin rounded-full border-2 border-white border-t-transparent" />
            ) : (
              "Ask"
            )}
          </button>
        </div>
      </form>
    </div>
  );
}

function MessageBubble({ message }: { message: ChatMessage }) {
  const isUser = message.role === "user";
  const [stepsExpanded, setStepsExpanded] = useState(false);

  return (
    <div className={`flex ${isUser ? "justify-end" : "justify-start"}`}>
      <div
        className={`max-w-[95%] rounded-lg px-3 py-2 text-sm ${
          isUser
            ? "bg-blue-600 text-white"
            : "bg-gray-100 text-gray-900"
        }`}
      >
        {/* Agent steps (collapsible) */}
        {!isUser && message.steps && message.steps.length > 0 && (
          <div className="mb-2">
            <button
              onClick={() => setStepsExpanded(!stepsExpanded)}
              className="flex items-center gap-1 text-xs text-gray-500 hover:text-gray-700"
            >
              <span className={`transition-transform ${stepsExpanded ? "rotate-90" : ""}`}>
                &#9654;
              </span>
              {message.steps.length} step{message.steps.length !== 1 ? "s" : ""}
            </button>
            {stepsExpanded && (
              <div className="mt-1 space-y-1 border-l-2 border-gray-200 pl-2">
                {message.steps.map((step, i) => (
                  <StepItem key={i} step={step} />
                ))}
              </div>
            )}
          </div>
        )}

        {/* Loading indicator */}
        {message.loading && !message.content && (
          <div className="flex items-center gap-2 text-gray-500">
            <span className="inline-block h-3 w-3 animate-spin rounded-full border-2 border-gray-400 border-t-transparent" />
            <span className="text-xs">
              {message.steps && message.steps.length > 0
                ? message.steps[message.steps.length - 1].message
                : "Thinking..."}
            </span>
          </div>
        )}

        {/* Answer content */}
        {message.content && (
          <div className="whitespace-pre-wrap break-words leading-relaxed">
            <SimpleMarkdown text={message.content} />
          </div>
        )}
      </div>
    </div>
  );
}

function StepItem({ step }: { step: AiStep }) {
  const icon =
    step.type === "thinking"
      ? "\u{1F914}"
      : step.type === "toolCall"
        ? "\u{1F527}"
        : "\u{2705}";

  return (
    <div className="text-xs text-gray-500">
      <span className="mr-1">{icon}</span>
      {step.message}
    </div>
  );
}

/** Very basic markdown renderer for bold, newlines, and bullet points. */
function SimpleMarkdown({ text }: { text: string }) {
  // Process the text into segments
  const lines = text.split("\n");

  return (
    <>
      {lines.map((line, i) => {
        // Bullet points
        if (line.startsWith("- ") || line.startsWith("* ")) {
          return (
            <div key={i} className="ml-2 flex gap-1">
              <span>&bull;</span>
              <span><InlineMarkdown text={line.slice(2)} /></span>
            </div>
          );
        }

        // Headers
        if (line.startsWith("### ")) {
          return (
            <div key={i} className="font-semibold mt-2 mb-1">
              <InlineMarkdown text={line.slice(4)} />
            </div>
          );
        }
        if (line.startsWith("## ")) {
          return (
            <div key={i} className="font-bold mt-2 mb-1">
              <InlineMarkdown text={line.slice(3)} />
            </div>
          );
        }

        // Empty line = paragraph break
        if (line.trim() === "") {
          return <div key={i} className="h-2" />;
        }

        return (
          <div key={i}>
            <InlineMarkdown text={line} />
          </div>
        );
      })}
    </>
  );
}

/** Renders inline bold (**text**) markers. */
function InlineMarkdown({ text }: { text: string }) {
  const parts = text.split(/(\*\*[^*]+\*\*)/g);
  return (
    <>
      {parts.map((part, i) => {
        if (part.startsWith("**") && part.endsWith("**")) {
          return (
            <strong key={i} className="font-semibold">
              {part.slice(2, -2)}
            </strong>
          );
        }
        return <span key={i}>{part}</span>;
      })}
    </>
  );
}
