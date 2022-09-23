import React, { useState } from 'react';

const Form = () => {
  const [srcId, setSrcId] = useState('');
  const [destId, setDestId] = useState('');

  const handleSubmit = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    console.log('srcId', srcId);
    console.log('destId', destId);
    // const response = await fetch('/api/transfer', {
    //   method: 'POST',
    //   headers: {
    //     'Content-Type': 'application/json',
    //   },
    //   body: JSON.stringify({
    //     srcId,
    //     destId,
    //   }),
    // });
    // const data = await response.json();
    // console.log(data);
  };

  return (
    <form onSubmit={handleSubmit}>
      <label>
        Source ID:
        <input
          type='text'
          // name='username'
          value={srcId}
          onChange={(e) => setSrcId(e.target.value)}
        />
      </label>
      <label>
        Destination ID:
        <input
          type='text'
          // name='age'
          value={destId}
          onChange={(e) => setDestId(e.target.value)}
        />
      </label>
      <input type='submit' />
    </form>
  );
};

export default Form;
