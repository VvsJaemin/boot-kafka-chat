import React, { useState } from "react";
import SockJsClient from "react-stomp";
import chatApi from "./services/chatapi";


import Chat from "./pages/Chat";
import Input from "./pages/Input";
import Login from "./pages/Login";

function App() {
  const [messages, setMessages] = useState([]);
  const [user, setUser] = useState(null);

  const onMessageReceived = (msg) => {
    console.log("New Message Received!!", msg);
    setMessages(messages.concat(msg));
  };

  const handleLoginSubmit = (name) => {
    setUser({ name: name, color: randomColor() });
  };

  const handleMessageSubmit = (msg) => {
    chatApi
      .sendMessage(user.name, msg)
      .then((res) => {
        console.log("sent", res);
      })
      .catch((e) => {
        console.log(e);
      });
  };

  const randomColor = () => {
    return "#" + Math.floor(Math.random() * 0xffffff).toString(16);
  };

  return (
    <>
      {user !== null ? (
        <div className="chat-container">
          <SockJsClient
            url={"http://localhost:6002/my-chat/"}
            topics={["/topic/group"]}
            onConnect={console.log("connected!")}
            onDisconnect={console.log("disconnected!")}
            onMessage={(msg) => onMessageReceived(msg)}
            debug={false}
          />
          <Chat messages={messages} currentUser={user} />
          <Input handleOnSubmit={handleMessageSubmit} />
        </div>
      ) : (
        <Login handleOnSubmit={handleLoginSubmit} />
      )}
    </>
  );
}

export default App;