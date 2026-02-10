import { useCallback, useRef, useState } from "react";

/** Types for AI agent events streamed via SSE. */
export interface AiThinkingEvent {
  type: "thinking";
  message: string;
}

export interface AiToolCallEvent {
  type: "toolCall";
  tool: string;
  params: Record<string, unknown>;
}

export interface AiToolResultEvent {
  type: "toolResult";
  tool: string;
  summary: string;
}

export interface AiAnswerEvent {
  type: "answer";
  text: string;
}

export interface AiErrorEvent {
  type: "error";
  message: string;
}

export interface AiDoneEvent {
  type: "done";
}

export type AiEvent =
  | AiThinkingEvent
  | AiToolCallEvent
  | AiToolResultEvent
  | AiAnswerEvent
  | AiErrorEvent
  | AiDoneEvent;

/** A message in the chat history. */
export interface ChatMessage {
  id: string;
  role: "user" | "assistant";
  content: string;
  /** Agent steps shown while processing (tool calls, thinking). */
  steps?: AiStep[];
  /** Whether this message is still being streamed. */
  loading?: boolean;
}

/** A single step the agent took. */
export interface AiStep {
  type: "thinking" | "toolCall" | "toolResult";
  message: string;
  tool?: string;
  params?: Record<string, unknown>;
}

/** Hook for managing AI chat state and SSE streaming. */
export function useAiChat() {
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [loading, setLoading] = useState(false);
  const abortRef = useRef<AbortController | null>(null);

  const ask = useCallback(async (question: string) => {
    // Cancel any in-flight request
    abortRef.current?.abort();

    const userMsg: ChatMessage = {
      id: `user-${Date.now()}`,
      role: "user",
      content: question,
    };

    const assistantId = `assistant-${Date.now()}`;
    const assistantMsg: ChatMessage = {
      id: assistantId,
      role: "assistant",
      content: "",
      steps: [],
      loading: true,
    };

    setMessages((prev) => [...prev, userMsg, assistantMsg]);
    setLoading(true);

    const controller = new AbortController();
    abortRef.current = controller;

    try {
      const resp = await fetch(
        `/api/ai/ask?q=${encodeURIComponent(question)}`,
        { signal: controller.signal },
      );

      if (!resp.ok) {
        const err = await resp.json().catch(() => ({ error: "Request failed" }));
        setMessages((prev) =>
          prev.map((m) =>
            m.id === assistantId
              ? { ...m, content: `Error: ${err.error ?? "Request failed"}`, loading: false }
              : m,
          ),
        );
        setLoading(false);
        return;
      }

      const reader = resp.body?.getReader();
      if (!reader) {
        setMessages((prev) =>
          prev.map((m) =>
            m.id === assistantId
              ? { ...m, content: "Error: No response body", loading: false }
              : m,
          ),
        );
        setLoading(false);
        return;
      }

      const decoder = new TextDecoder();
      let buffer = "";

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });

        // Parse SSE events from buffer
        const lines = buffer.split("\n");
        buffer = lines.pop() ?? "";

        for (const line of lines) {
          if (!line.startsWith("data: ")) continue;
          const json = line.slice(6).trim();
          if (!json) continue;

          try {
            const event: AiEvent = JSON.parse(json);
            setMessages((prev) =>
              prev.map((m) => {
                if (m.id !== assistantId) return m;
                return applyEvent(m, event);
              }),
            );

            if (event.type === "done" || event.type === "answer") {
              // Mark as done
              setMessages((prev) =>
                prev.map((m) =>
                  m.id === assistantId ? { ...m, loading: false } : m,
                ),
              );
            }
          } catch {
            // Skip malformed events
          }
        }
      }
    } catch (err) {
      if ((err as Error).name !== "AbortError") {
        setMessages((prev) =>
          prev.map((m) =>
            m.id === assistantId
              ? { ...m, content: `Error: ${(err as Error).message}`, loading: false }
              : m,
          ),
        );
      }
    } finally {
      setLoading(false);
      setMessages((prev) =>
        prev.map((m) =>
          m.id === assistantId ? { ...m, loading: false } : m,
        ),
      );
    }
  }, []);

  const clear = useCallback(() => {
    abortRef.current?.abort();
    setMessages([]);
    setLoading(false);
  }, []);

  return { messages, loading, ask, clear };
}

/** Apply an SSE event to update an assistant message in-place. */
function applyEvent(msg: ChatMessage, event: AiEvent): ChatMessage {
  switch (event.type) {
    case "thinking":
      return {
        ...msg,
        steps: [...(msg.steps ?? []), { type: "thinking", message: event.message }],
      };
    case "toolCall":
      return {
        ...msg,
        steps: [
          ...(msg.steps ?? []),
          { type: "toolCall", message: `Calling ${event.tool}...`, tool: event.tool, params: event.params },
        ],
      };
    case "toolResult":
      return {
        ...msg,
        steps: [
          ...(msg.steps ?? []),
          { type: "toolResult", message: event.summary, tool: event.tool },
        ],
      };
    case "answer":
      return { ...msg, content: event.text };
    case "error":
      return { ...msg, content: `Error: ${event.message}`, loading: false };
    default:
      return msg;
  }
}
