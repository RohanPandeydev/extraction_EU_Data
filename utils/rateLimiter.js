function sleep(baseMs = 5000, jitterMs = 5000) {
  const delay = baseMs + Math.floor(Math.random() * jitterMs);
  return new Promise((resolve) => setTimeout(resolve, delay));
}

module.exports = { sleep };
