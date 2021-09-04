import React, { useState } from "react";

function Login({ handleOnSubmit }) {
  const [name, setName] = useState("");

  const handleOnChange = (e) => {
    setName(e.target.value);
  };

  const handleSubmit = () => {
    handleOnSubmit(name);
  };
  
  return (
    <div>
      <form onSubmit={handleSubmit}>
        <input
          placeholder="사용할 닉네임을 입력하세요."
          value={name}
          onChange={handleOnChange}
        />
        <button type="submit">Go!</button>
      </form>
    </div>
  );
}

export default Login;