import gradio as gr
from agent import generate_response

chat_history = []


def chat(user_input):
    chat_history.append(("User", user_input))
    try:
        bot_reply = generate_response(user_input)
    except Exception as e:
        bot_reply = f"Error: {e}"
    chat_history.append(("Assistant", bot_reply))
    messages = [f"**{role}:** {text}" for role, text in chat_history]
    return "\n\n".join(messages)


if __name__ == "__main__":
    gr.Interface(
        fn=chat,
        inputs=gr.Textbox(
            lines=2, placeholder="Ask something like: Show tiles with highest area in Himalayas..."),
        outputs="markdown",
        title="MonkDB Geospatial Chat",
        description="Chat-based natural language interface for MonkDB raster tile analytics with TinyLlama."
    ).launch()
