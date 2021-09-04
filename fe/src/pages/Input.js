import React, { useState } from "react";

function Input({ handleOnSubmit }) {
  const [msg, setMsg] = useState("");

  const handleOnChange = (e) => {
    setMsg(e.target.value);
  };
  const handleSubmit = async (e) => {
    e.preventDefault();
    handleOnSubmit(msg);
    setMsg("");
  };

  return (
    <div className="chat-bottom">
      <form onSubmit={handleSubmit}>
        <input
          placeholder="내용을 입력하세요."
          value={msg}
          onChange={handleOnChange}
          onKeyPress={(e) => {
            if (e.key === "Enter") {
              handleSubmit(e);
            }
          }}
        />
        <button type="submit">전송</button>
      </form>
    </div>
  );
}

export default Input;