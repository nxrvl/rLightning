/**
 * rLightning Documentation - Scroll Animations & Visual Effects
 */

(function () {
  "use strict";

  // --- Intersection Observer for fade-in animations ---
  function initFadeAnimations() {
    var targets = document.querySelectorAll(".rl-fade-in");
    if (!targets.length) return;

    if (!("IntersectionObserver" in window)) {
      // Fallback: show everything immediately
      targets.forEach(function (el) {
        el.classList.add("rl-visible");
      });
      return;
    }

    var observer = new IntersectionObserver(
      function (entries) {
        entries.forEach(function (entry) {
          if (entry.isIntersecting) {
            entry.target.classList.add("rl-visible");

            // Also trigger child cards in stagger containers
            var cards = entry.target.querySelectorAll(".rl-card");
            cards.forEach(function (card) {
              card.classList.add("rl-visible");
            });

            observer.unobserve(entry.target);
          }
        });
      },
      {
        threshold: 0.1,
        rootMargin: "0px 0px -40px 0px",
      }
    );

    targets.forEach(function (el) {
      observer.observe(el);
    });
  }

  // --- Stagger children fade-in ---
  function initStaggerChildren() {
    var containers = document.querySelectorAll(".rl-stagger");
    containers.forEach(function (container) {
      var cards = container.querySelectorAll(".rl-card");
      cards.forEach(function (card) {
        card.classList.add("rl-fade-in");
      });
    });
  }

  // --- Initialize on DOM ready ---
  function init() {
    initStaggerChildren();
    initFadeAnimations();
  }

  // Support both instant navigation (MkDocs Material) and regular page loads
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }

  // Re-init on MkDocs instant navigation
  if (typeof document$ !== "undefined") {
    document$.subscribe(function () {
      init();
    });
  }
})();
