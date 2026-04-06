/* global L */
(function () {
  var quizJsonEl = document.getElementById("home-quiz-json");
  var quizData = [];
  try {
    quizData = quizJsonEl ? JSON.parse(quizJsonEl.textContent) : [];
  } catch (e) {
    quizData = [];
  }

  function parseSq(str) {
    if (!str) return null;
    var s = String(str).replace(/,/g, "");
    var m = s.match(/(\d+(?:\.\d+)?)/);
    return m ? parseFloat(m[1]) : null;
  }

  function parsePriceRough(str) {
    if (!str) return null;
    var n = String(str).replace(/[^\d.]/g, "");
    if (!n) return null;
    return parseFloat(n);
  }

  function scoreMatch(p, a) {
    var s = 0;
    var ps = parseSq(p.size);
    if (ps && a.sqm) {
      var diff = Math.abs(ps - a.sqm) / Math.max(a.sqm, 1);
      if (diff < 0.2) s += 45;
      else if (diff < 0.4) s += 28;
      else if (diff < 0.65) s += 12;
    }
    if (a.suburb === "Either" || !a.suburb) s += 15;
    else if (p.area && p.area.indexOf(a.suburb) !== -1) s += 25;
    var blob = (p.description || "") + " " + (p.features || "");
    var pPhase = (p.power_phase || "").trim();
    var pAmps = (p.power_amps || "").trim();
    if (a.yard && /yard|yard space|open area|paved/i.test(blob)) s += 10;
    if (a.shutter && /shutter|roller/i.test(blob)) s += 10;
    if (a.office && /office|mezzanine/i.test(blob)) s += 10;
    if (a.power === "3phase" && pPhase === "3-phase") s += 12;
    else if (a.power === "heavy" && (pAmps || /heavy|high|kva/i.test(blob))) s += 10;
    else if (a.power === "standard" && (pPhase === "single-phase" || /(100|60)/i.test(pAmps))) s += 8;
    else if (a.power === "any" && (pPhase || pAmps)) s += 4;

    var pr = parsePriceRough(p.price);
    if (pr && a.budget) {
      var ratio = pr / a.budget;
      if (ratio >= 0.7 && ratio <= 1.35) s += 20;
      else if (ratio >= 0.5 && ratio <= 1.6) s += 10;
    }
    return s;
  }

  function buildQuizUI() {
    var host = document.getElementById("quiz-steps");
    if (!host) return;
    host.innerHTML =
      '<div class="quiz-field"><label>Required area (m²)</label><input type="number" id="q-sqm" min="50" step="10" value="500"></div>' +
      '<div class="quiz-field"><label>Budget (R / month)</label><input type="number" id="q-budget" min="0" step="1000" value="80000"></div>' +
      '<div class="quiz-field"><label>Preferred suburb</label><select id="q-suburb"><option value="Maitland">Maitland</option><option value="Paarden Eiland">Paarden Eiland</option><option value="Either">Either / no preference</option></select></div>' +
      '<div class="quiz-field"><label>Yard space needed?</label><select id="q-yard"><option value="no">No</option><option value="yes">Yes</option></select></div>' +
      '<div class="quiz-field"><label>Roller shutter doors?</label><select id="q-shutter"><option value="no">Not essential</option><option value="yes">Important</option></select></div>' +
      '<div class="quiz-field"><label>Office component?</label><select id="q-office"><option value="no">Minimal</option><option value="yes">Required</option></select></div>' +
      '<div class="quiz-field"><label>Power</label><select id="q-power">' +
      '<option value="any">Flexible</option><option value="standard">Standard</option><option value="3phase">3-phase</option><option value="heavy">Heavy / high amps</option></select></div>' +
      '<button type="button" class="primary-btn quiz-run-btn" id="quiz-run">Find matches</button>';

    document.getElementById("quiz-run").addEventListener("click", function () {
      var a = {
        sqm: Number(document.getElementById("q-sqm").value) || 500,
        budget: Number(document.getElementById("q-budget").value) || 80000,
        suburb: document.getElementById("q-suburb").value,
        yard: document.getElementById("q-yard").value === "yes",
        shutter: document.getElementById("q-shutter").value === "yes",
        office: document.getElementById("q-office").value === "yes",
        power: document.getElementById("q-power").value
      };

      var scored = quizData
        .map(function (p) {
          return { p: p, score: scoreMatch(p, a) };
        })
        .sort(function (x, y) {
          return y.score - x.score;
        });

      var top = scored.slice(0, 3).filter(function (x) {
        return x.score > 0;
      });
      if (!top.length) {
        top = scored.slice(0, 3);
      }

      var cards = document.getElementById("quiz-match-cards");
      var res = document.getElementById("quiz-results");
      host.hidden = true;
      cards.innerHTML = "";
      top.forEach(function (item, i) {
        var p = item.p;
        var div = document.createElement("article");
        div.className = "quiz-match-card";
        div.style.animationDelay = i * 0.12 + "s";
        var imgHtml = p.cardImage
          ? '<img src="/uploads/' +
            p.cardImage.replace(/"/g, "") +
            '" alt="">'
          : '<div class="quiz-match-card__ph">No image</div>';
        div.innerHTML =
          '<a href="' +
          p.url +
          '" class="quiz-match-card__link">' +
          imgHtml +
          '<div class="quiz-match-card__body"><h4>' +
          escapeHtml(p.name) +
          "</h4><p>" +
          escapeHtml(p.area) +
          " · " +
          escapeHtml(p.size) +
          "</p><p class='quiz-match-card__price'>" +
          escapeHtml(p.price) +
          "</p></div></a>";
        cards.appendChild(div);
      });
      res.hidden = false;
    });
  }

  function escapeHtml(s) {
    var d = document.createElement("div");
    d.textContent = s;
    return d.innerHTML;
  }

  var quizModal = document.getElementById("quiz-modal");

  function openQuizModal() {
    if (!quizModal) return;
    quizModal.hidden = false;
    var qs = document.getElementById("quiz-steps");
    if (qs) qs.hidden = false;
    var qr = document.getElementById("quiz-results");
    if (qr) qr.hidden = true;
    var mc = document.getElementById("quiz-match-cards");
    if (mc) mc.innerHTML = "";
    buildQuizUI();
    document.body.style.overflow = "hidden";
  }

  function closeQuizModal() {
    if (!quizModal) return;
    quizModal.hidden = true;
    document.body.style.overflow = "";
  }

  function initQuizModal() {
    var fab = document.getElementById("eagle-chat-fab");
    var bd = document.getElementById("quiz-modal-backdrop");
    var cl = document.getElementById("quiz-modal-close");
    var clf = document.getElementById("quiz-modal-close-footer");
    if (fab) fab.addEventListener("click", openQuizModal);
    if (bd) bd.addEventListener("click", closeQuizModal);
    if (cl) cl.addEventListener("click", closeQuizModal);
    if (clf) clf.addEventListener("click", closeQuizModal);
  }

  function initFeaturedGalleryCycle() {
    if (window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches) {
      return;
    }
    var cards = [];
    document.querySelectorAll(".home-featured-card__media--cycle").forEach(function (media) {
      var raw = media.getAttribute("data-feature-gallery");
      if (!raw) return;
      var urls;
      try {
        urls = JSON.parse(raw);
      } catch (e) {
        return;
      }
      if (!urls || urls.length < 2) return;
      var a = media.querySelector(".home-featured-card__img--a");
      var b = media.querySelector(".home-featured-card__img--b");
      if (!a || !b) return;
      cards.push({ media: media, urls: urls, a: a, b: b, i: 0 });
    });
    if (cards.length === 0) return;

    var stepMs = 2400;
    var round = 0;
    var intervalId = null;
    var started = false;

    function advanceOne(card) {
      var urls = card.urls;
      var next = (card.i + 1) % urls.length;
      if (card.media.classList.contains("is-front-b")) {
        card.a.src = urls[next];
      } else {
        card.b.src = urls[next];
      }
      card.media.classList.toggle("is-front-b");
      card.i = next;
    }

    function tick() {
      advanceOne(cards[round]);
      round = (round + 1) % cards.length;
    }

    function startRotation() {
      if (intervalId != null) return;
      intervalId = setInterval(tick, stepMs);
    }

    var observeEl = document.getElementById("home-featured-observe");
    if (!observeEl || typeof IntersectionObserver === "undefined") {
      setTimeout(function () {
        startRotation();
      }, 2000);
      return;
    }

    var obs = new IntersectionObserver(
      function (entries) {
        entries.forEach(function (en) {
          if (started || !en.isIntersecting) return;
          if (en.intersectionRatio < 0.98) return;
          started = true;
          obs.disconnect();
          setTimeout(function () {
            startRotation();
          }, 2000);
        });
      },
      { root: null, rootMargin: "0px", threshold: [0, 0.25, 0.5, 0.75, 0.9, 0.95, 1] }
    );

    obs.observe(observeEl);
  }

  initFeaturedGalleryCycle();
  initQuizModal();

  function initHomeFilterScrollToResults() {
    var form = document.querySelector(".home-filter-form");
    if (!form) return;
    form.addEventListener("submit", function (e) {
      e.preventDefault();
      try {
        var params = new URLSearchParams(new FormData(form));
        window.location.assign("/?" + params.toString() + "#home-search-results");
      } catch (err) {
        form.submit();
      }
    });
  }

  function scrollToHashSearchResults() {
    if (location.hash !== "#home-search-results") return;
    var el = document.getElementById("home-search-results");
    if (!el) return;
    var reduce =
      window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    requestAnimationFrame(function () {
      var y = el.getBoundingClientRect().top + window.pageYOffset - 12;
      window.scrollTo({ top: Math.max(0, y), behavior: reduce ? "auto" : "smooth" });
    });
  }

  initHomeFilterScrollToResults();
  scrollToHashSearchResults();
  window.addEventListener("hashchange", scrollToHashSearchResults);

  var eagleLauncher = document.getElementById("eagle-chat-launcher");
  var eagleLauncherDismiss = document.getElementById("eagle-chat-launcher-dismiss");
  var EAGLE_HIDE_KEY = "eagleChatLauncherHidden";
  try {
    /* Legacy: dismiss used to persist in localStorage forever — clear so the FAB is not stuck off. */
    if (window.localStorage) {
      window.localStorage.removeItem(EAGLE_HIDE_KEY);
    }
  } catch (e0) {
    /* ignore */
  }
  try {
    if (eagleLauncher && window.sessionStorage && window.sessionStorage.getItem(EAGLE_HIDE_KEY) === "1") {
      eagleLauncher.setAttribute("hidden", "");
    }
  } catch (e1) {
    /* ignore */
  }
  if (eagleLauncherDismiss && eagleLauncher) {
    eagleLauncherDismiss.addEventListener("click", function (e) {
      e.preventDefault();
      e.stopPropagation();
      try {
        if (window.sessionStorage) {
          window.sessionStorage.setItem(EAGLE_HIDE_KEY, "1");
        }
      } catch (e2) {
        /* ignore */
      }
      eagleLauncher.setAttribute("hidden", "");
    });
  }

  document.addEventListener("keydown", function (e) {
    if (e.ctrlKey && e.shiftKey && (e.key === "a" || e.key === "A")) {
      e.preventDefault();
      window.location.href = "/admin/login";
    }
  });
})();
