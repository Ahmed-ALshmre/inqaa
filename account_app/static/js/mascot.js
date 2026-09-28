(() => {
  const mascot = document.querySelector('[data-soof-mascot]');
  const button = document.querySelector('[data-mascot-action]');
  if (!mascot || !button) return;

  const images = {
    idle: '/static/icons/soof-mascot-idle.png?v=1',
    wave: '/static/icons/soof-mascot-wave.png?v=1',
    celebrate: '/static/icons/soof-mascot-celebrate.png?v=1',
  };
  let resetTimer;

  function show(state, duration = 2300) {
    clearTimeout(resetTimer);
    mascot.src = images[state] || images.idle;
    button.classList.remove('mascot-active');
    void button.offsetWidth;
    button.classList.add('mascot-active');
    resetTimer = setTimeout(() => {
      mascot.src = images.idle;
      button.classList.remove('mascot-active');
    }, duration);
  }

  button.addEventListener('click', () => show('wave'));
  document.addEventListener('soof:celebrate', () => show('celebrate', 6000));
})();
