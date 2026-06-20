// Vercel Edge Function — proxies the Groq API and streams the reply.
// The GROQ_API_KEY never reaches the browser; it lives only in this function.

export const config = { runtime: "edge" };

// The persona lives on the server so the client can't tamper with it.
const SYSTEM_PROMPT = `You are Dr. Strata, a senior field and academic geologist with 30+ years of
experience across mineralogy, petrology, structural geology, sedimentology, stratigraphy,
geophysics, hydrogeology, volcanology, economic/mining geology, petroleum geology, geochemistry,
geomorphology, plate tectonics, and geohazards.

How you answer:
- Treat every question as if a curious student, field engineer, or fellow scientist asked it.
- Be clear and practical. Lead with the direct answer, then add the "why" with real-world examples.
- Use plain language first; introduce technical terms only after explaining them.
- When a question is ambiguous (location, scale, rock type, context), give the best general answer
  AND note what extra detail would sharpen it — but never refuse to answer.
- Be honest about uncertainty and scientific debate. Distinguish established consensus from open questions.
- Keep answers focused and mobile-friendly: short paragraphs, occasional bullet points for lists.
- Stay strictly within geology and adjacent earth sciences. If asked something unrelated,
  briefly steer back to your domain.`;

const GROQ_URL = "https://api.groq.com/openai/v1/chat/completions";
const MODEL = "llama-3.3-70b-versatile"; // swap to "openai/gpt-oss-120b" etc. anytime

export default async function handler(req) {
  if (req.method !== "POST") {
    return new Response("Method Not Allowed", { status: 405 });
  }

  const apiKey = process.env.GROQ_API_KEY;
  if (!apiKey) {
    return json({ error: "Server not configured: GROQ_API_KEY is missing." }, 500);
  }

  let body;
  try {
    body = await req.json();
  } catch {
    return json({ error: "Invalid JSON body." }, 400);
  }

  const incoming = Array.isArray(body.messages) ? body.messages : [];
  // Keep only well-formed user/assistant turns, and cap history to control tokens.
  const history = incoming
    .filter(
      (m) =>
        m &&
        (m.role === "user" || m.role === "assistant") &&
        typeof m.content === "string"
    )
    .slice(-20);

  const messages = [{ role: "system", content: SYSTEM_PROMPT }, ...history];

  let groqRes;
  try {
    groqRes = await fetch(GROQ_URL, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        model: MODEL,
        messages,
        temperature: 0.4,
        max_tokens: 1200,
        stream: true,
      }),
    });
  } catch (e) {
    return json({ error: "Could not reach Groq." }, 502);
  }

  if (!groqRes.ok || !groqRes.body) {
    const detail = await groqRes.text().catch(() => "");
    return json({ error: "Groq returned an error.", detail: detail.slice(0, 500) }, 502);
  }

  // Parse Groq's SSE stream and re-emit only the plain text deltas.
  const encoder = new TextEncoder();
  const decoder = new TextDecoder();

  const stream = new ReadableStream({
    async start(controller) {
      const reader = groqRes.body.getReader();
      let buffer = "";
      try {
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });
          const lines = buffer.split("\n");
          buffer = lines.pop() ?? "";
          for (const line of lines) {
            const trimmed = line.trim();
            if (!trimmed.startsWith("data:")) continue;
            const data = trimmed.slice(5).trim();
            if (data === "[DONE]") {
              controller.close();
              return;
            }
            try {
              const parsed = JSON.parse(data);
              const delta = parsed.choices?.[0]?.delta?.content;
              if (delta) controller.enqueue(encoder.encode(delta));
            } catch {
              // ignore keep-alive / partial chunks
            }
          }
        }
        controller.close();
      } catch (err) {
        controller.error(err);
      }
    },
  });

  return new Response(stream, {
    headers: {
      "Content-Type": "text/plain; charset=utf-8",
      "Cache-Control": "no-cache, no-transform",
      "X-Accel-Buffering": "no",
    },
  });
}

function json(obj, status) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}
