import React from "react";

function Chat({ messages, currentUser }) {

  const formattingTimestamp = (timestamp) => {
    const date = new Date(timestamp);
    let hour = date.getHours() < 10 ? `0${date.getHours()}` : date.getHours();
    let min =
      date.getMinutes() < 10 ? `0${date.getMinutes()}` : date.getMinutes();
    return `${hour}:${min}`;
  };

  return (
    <div className="chat-middle">
      {messages.map((msg) => (
        <li
          className={`chat-bubble ${
            msg.author === currentUser.name ? "send" : "receive"
          }`}
        >
          <span>{msg.author}</span>
          <p>{msg.content}</p>
          <span>{formattingTimestamp(msg.timestamp)}</span>
        </li>
      ))}
    </div>
  );
}

export default Chat;