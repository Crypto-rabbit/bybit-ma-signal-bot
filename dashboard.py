<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>MA Signal Dashboard</title>
  <style>
    body {
      background-color: #000;
      color: #fff;
      font-family: monospace;
      padding: 20px;
    }
    .signal {
      margin-bottom: 10px;
    }
    .buy {
      color: #00ff00;
    }
    .sell {
      color: #ff4444;
    }
  </style>
</head>
<body>
  <h1>📡 MA Signals</h1>
  <div id="signals"></div>

  <script>
    const ws = new WebSocket('ws://localhost:6789');
    const signalsDiv = document.getElementById('signals');

    ws.onmessage = (event) => {
      const data = JSON.parse(event.data);
      const signalEl = document.createElement('div');
      signalEl.classList.add('signal');

      const type = data.type;
      const text = `${type} ${data.symbol} (${data.ma || ''}) @ ${data.price}`;

      if (type === 'BUY') signalEl.classList.add('buy');
      else if (type === 'SELL') signalEl.classList.add('sell');

      signalEl.textContent = text;
      signalsDiv.prepend(signalEl);
    };
  </script>
</body>
</html>
