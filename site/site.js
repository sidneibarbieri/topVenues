(function () {
  "use strict";
  var root = document.documentElement;

  function remember(key, value) {
    try { localStorage.setItem(key, value); } catch (unavailable) { /* private mode: the choice lasts for this page only */ }
  }

  function applyLanguage(language) {
    root.setAttribute("data-lang", language);
    root.lang = language === "pt" ? "pt-BR" : "en";
    document.title = root.getAttribute("data-title-" + language);
    var description = document.querySelector('meta[name="description"]');
    if (description) description.setAttribute("content", root.getAttribute("data-desc-" + language));
    document.querySelectorAll("[data-set-lang]").forEach(function (button) {
      button.setAttribute("aria-pressed", String(button.getAttribute("data-set-lang") === language));
    });
    document.querySelectorAll("[data-label-" + language + "]").forEach(function (element) {
      element.setAttribute("aria-label", element.getAttribute("data-label-" + language));
    });
    document.querySelectorAll("video").forEach(syncCaptions);
  }

  // A track set to "showing" is fetched at once, so captions are chosen only
  // after playback starts: nothing leaves this page for a third party before.
  function syncCaptions(video) {
    if (!video.hasAttribute("data-started")) return;
    var language = root.getAttribute("data-lang");
    Array.prototype.forEach.call(video.textTracks, function (track) {
      track.mode = track.language.indexOf(language) === 0 ? "showing" : "disabled";
    });
  }

  document.querySelectorAll("video").forEach(function (video) {
    video.addEventListener("play", function () {
      video.setAttribute("data-started", "");
      syncCaptions(video);
    });
  });

  document.querySelectorAll("[data-set-lang]").forEach(function (button) {
    button.addEventListener("click", function () {
      var language = button.getAttribute("data-set-lang");
      remember("tv-lang", language);
      applyLanguage(language);
    });
  });
  applyLanguage(root.getAttribute("data-lang") || "en");

  var themeToggle = document.querySelector("[data-theme-toggle]");
  if (themeToggle) themeToggle.addEventListener("click", function () {
    var current = root.getAttribute("data-theme") ||
      (window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light");
    var next = current === "dark" ? "light" : "dark";
    root.setAttribute("data-theme", next);
    remember("tv-theme", next);
  });

  function copyText(text) {
    if (navigator.clipboard && window.isSecureContext) return navigator.clipboard.writeText(text);
    return new Promise(function (resolve, reject) {
      var scratch = document.createElement("textarea");
      scratch.value = text;
      scratch.setAttribute("readonly", "");
      scratch.style.position = "fixed";
      scratch.style.opacity = "0";
      document.body.appendChild(scratch);
      scratch.select();
      var copied = document.execCommand("copy");
      document.body.removeChild(scratch);
      if (copied) resolve(); else reject(new Error("copy command was refused"));
    });
  }

  document.querySelectorAll("[data-copy]").forEach(function (button) {
    button.addEventListener("click", function () {
      var code = button.closest(".code").querySelector("pre code");
      copyText(code.textContent).then(function () {
        var original = button.innerHTML;
        button.classList.add("ok");
        button.innerHTML = '<span data-l="en">Copied</span><span data-l="pt">Copiado</span>';
        setTimeout(function () { button.classList.remove("ok"); button.innerHTML = original; }, 1600);
      });
    });
  });

  document.querySelectorAll(".tabs").forEach(function (group) {
    var tabs = Array.prototype.slice.call(group.querySelectorAll('[role="tab"]'));
    function select(chosen) {
      tabs.forEach(function (tab) {
        var isChosen = tab === chosen;
        tab.setAttribute("aria-selected", String(isChosen));
        tab.tabIndex = isChosen ? 0 : -1;
        document.getElementById(tab.getAttribute("aria-controls")).hidden = !isChosen;
      });
    }
    tabs.forEach(function (tab, position) {
      tab.addEventListener("click", function () { select(tab); });
      tab.addEventListener("keydown", function (event) {
        var step = event.key === "ArrowRight" ? 1 : event.key === "ArrowLeft" ? -1 : 0;
        if (!step) return;
        var next = tabs[(position + step + tabs.length) % tabs.length];
        select(next);
        next.focus();
        event.preventDefault();
      });
    });
    if (/Win/.test(navigator.platform || navigator.userAgent)) select(tabs[tabs.length - 1]);
  });
})();
