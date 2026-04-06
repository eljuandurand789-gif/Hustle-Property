(function () {
  window.navigateBrowseByArea = function (selectEl) {
    var form = selectEl.form;
    if (!form) return;
    var params = new URLSearchParams();
    ["home_search", "keywords", "status", "property_type", "amps_min", "height_min"].forEach(function (n) {
      var el = form.querySelector('[name="' + n + '"]');
      if (el && el.value !== "") params.set(n, el.value);
    });
    var q = params.toString();
    var v = selectEl.value;
    var resultsHash = "#home-search-results";
    if (!v) {
      window.location = "/" + (q ? "?" + q : "") + resultsHash;
      return;
    }
    var slug = v === "Maitland" ? "maitland" : "paarden-eiland";
    window.location = "/area/" + slug + (q ? "?" + q : "") + resultsHash;
  };
})();
