(function () {
  var root = document.documentElement;

  function stored(key) {
    try { return window.localStorage.getItem(key); } catch (unavailable) { return null; }
  }

  function requestedLanguage() {
    var query = (new URLSearchParams(location.search).get("lang") || "").toLowerCase();
    if (query.indexOf("pt") === 0) return "pt";
    if (query.indexOf("en") === 0) return "en";
    var remembered = stored("tv-lang");
    if (remembered === "en" || remembered === "pt") return remembered;
    return (navigator.language || "en").toLowerCase().indexOf("pt") === 0 ? "pt" : "en";
  }

  var language = requestedLanguage();
  root.setAttribute("data-lang", language);
  root.lang = language === "pt" ? "pt-BR" : "en";
  var theme = stored("tv-theme");
  if (theme === "light" || theme === "dark") root.setAttribute("data-theme", theme);
})();
