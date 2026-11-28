/*
 * apoHouze scanner engine.
 *
 *  - parse():   turns a decoded barcode into { code, expiry, batch, ... }.
 *               EU medicine packs carry a GS1 DataMatrix with GTIN, expiry date,
 *               batch and serial number; older packs only have an EAN/Code39.
 *  - start()/stop():  live camera scanning (needs HTTPS or localhost).
 *  - decodeFile():    decode a photo taken with the native camera app. This also
 *                     works over plain http://, where live camera access is blocked.
 *
 * No dependencies on the rest of the app, so parse() can be unit-tested in Node.
 */
(function (root) {
  'use strict';

  var GS = '\u001d';

  // ---------------------------------------------------------------- GS1 parsing
  // Application identifiers: fixed length (digits) or variable (max length).
  var FIXED = { '00': 18, '01': 14, '02': 14, '11': 6, '12': 6, '13': 6, '15': 6, '16': 6, '17': 6, '20': 2 };
  var VARIABLE = {
    '10': 20, '21': 20, '22': 20, '30': 8, '37': 8, '90': 30,
    '240': 30, '241': 30, '242': 6, '250': 30, '251': 30, '253': 30, '254': 20,
    '400': 30, '401': 30, '402': 17, '403': 30,
    '710': 20, '711': 20, '712': 20, '713': 20, '714': 20, '715': 20,
  };
  for (var n = 91; n <= 99; n++) VARIABLE[String(n)] = 90;

  function aiAt(s, i) {
    for (var len = 2; len <= 4; len++) {
      var c = s.substr(i, len);
      if (c.length === len && (FIXED[c] !== undefined || VARIABLE[c] !== undefined)) return c;
    }
    return null;
  }

  function gtinValid(digits) {
    if (!/^\d{8}$|^\d{12,14}$/.test(digits)) return false;
    var d = digits.split('').reverse(), sum = 0;
    for (var i = 1; i < d.length; i++) sum += (+d[i]) * (i % 2 === 1 ? 3 : 1);
    return (10 - (sum % 10)) % 10 === +d[0];
  }

  // "(01)0541...(17)271100" (human readable form) -> same object as the raw form.
  function parseBracketed(s) {
    var out = {}, re = /\((\d{2,4})\)([^(]*)/g, m, any = false;
    while ((m = re.exec(s))) { out[m[1]] = m[2]; any = true; }
    return any ? out : null;
  }

  function parseGS1(raw) {
    var s = String(raw).replace(/^\](d2|C1|E0|Q3)/, '').replace(/^\u001d/, '');
    var ais = /^\(\d{2,4}\)/.test(s) ? parseBracketed(s) : null;
    if (!ais) {
      ais = {};
      var i = 0;
      while (i < s.length) {
        if (s.charAt(i) === GS) { i++; continue; }
        var ai = aiAt(s, i);
        if (!ai) break;                       // unknown AI: keep what we have
        i += ai.length;
        var val;
        if (FIXED[ai] !== undefined) {
          val = s.substr(i, FIXED[ai]); i += FIXED[ai];
        } else {
          var j = s.indexOf(GS, i); if (j < 0) j = s.length;
          val = s.slice(i, j).slice(0, VARIABLE[ai]); i = j;
        }
        ais[ai] = val;
      }
    }
    return ais && ais['01'] ? ais : null;
  }

  // YYMMDD -> "YYYY-MM". Day 00 means "end of that month" (GS1), which is the same month.
  function expiryMonth(yymmdd) {
    var m = /^(\d{2})(\d{2})(\d{2})$/.exec(yymmdd || '');
    if (!m) return null;
    var mm = +m[2];
    if (mm < 1 || mm > 12) return null;
    return (2000 + (+m[1])) + '-' + m[2];
  }

  function pad14(digits) { return ('00000000000000' + digits).slice(-14); }

  /**
   * @param {string} raw     decoded text
   * @param {string} format  e.g. "DATA_MATRIX", "data_matrix", "EAN_13"
   * @returns {null | {kind:'gs1'|'gtin'|'code', code:string, raw:string, format:string,
   *                   expiry:string|null, batch:string, serial:string}}
   */
  function parse(raw, format) {
    if (raw == null) return null;
    var text = String(raw);
    if (!text.replace(/[\s\u001d]/g, '')) return null;
    var fmt = String(format || '').toUpperCase();
    var base = { raw: text, format: fmt, expiry: null, batch: '', serial: '' };

    var gs1 = parseGS1(text);
    if (gs1 && /^\d{14}$/.test(gs1['01'])) {
      base.kind = 'gs1';
      base.code = gs1['01'];
      base.expiry = expiryMonth(gs1['17']) || expiryMonth(gs1['15']);   // 15 = best before
      base.batch = (gs1['10'] || '').trim();
      base.serial = (gs1['21'] || '').trim();
      return base;
    }

    var digits = text.replace(/\s/g, '');
    if (/^\d+$/.test(digits) && gtinValid(digits)) {         // EAN-8 / UPC-A / EAN-13 / GTIN-14
      base.kind = 'gtin';
      base.code = pad14(digits);                              // same key as the DataMatrix GTIN of that pack
      return base;
    }

    // Anything else (Code 39 CNK/PZN, QR, ...): keep as an opaque pack code.
    var clean = text.replace(/[^A-Za-z0-9._-]/g, '').slice(0, 58);
    if (clean.length < 3) return null;
    base.kind = 'code';
    base.code = 'raw:' + clean;
    return base;
  }

  // ------------------------------------------------------------------- decoding
  var NATIVE_FORMATS = ['data_matrix', 'ean_13', 'ean_8', 'upc_a', 'code_128', 'code_39', 'qr_code'];
  var engine = null, enginePromise = null;

  function loadZXing() {
    if (root.ZXing) return Promise.resolve(root.ZXing);
    return new Promise(function (resolve, reject) {
      var s = document.createElement('script');
      s.src = '/vendor/zxing.min.js';
      s.onload = function () { root.ZXing ? resolve(root.ZXing) : reject(new Error('zxing missing')); };
      s.onerror = function () { reject(new Error('zxing load failed')); };
      document.head.appendChild(s);
    });
  }

  function zxingEngine(Z) {
    var hints = new Map();
    hints.set(Z.DecodeHintType.POSSIBLE_FORMATS, [
      Z.BarcodeFormat.DATA_MATRIX, Z.BarcodeFormat.EAN_13, Z.BarcodeFormat.EAN_8,
      Z.BarcodeFormat.UPC_A, Z.BarcodeFormat.CODE_128, Z.BarcodeFormat.CODE_39, Z.BarcodeFormat.QR_CODE,
    ]);
    hints.set(Z.DecodeHintType.TRY_HARDER, true);
    var reader = new Z.MultiFormatReader();
    reader.setHints(hints);
    return {
      name: 'zxing',
      decode: function (canvas) {
        var w = canvas.width, h = canvas.height;
        var rgba = canvas.getContext('2d', { willReadFrequently: true }).getImageData(0, 0, w, h).data;
        var lum = new Uint8ClampedArray(w * h);
        for (var i = 0, p = 0; i < lum.length; i++, p += 4) {
          lum[i] = (rgba[p] * 299 + rgba[p + 1] * 587 + rgba[p + 2] * 114) / 1000;
        }
        try {
          var res = reader.decode(new Z.BinaryBitmap(new Z.HybridBinarizer(new Z.RGBLuminanceSource(lum, w, h))));
          return Promise.resolve({ text: res.getText(), format: Z.BarcodeFormat[res.getBarcodeFormat()] });
        } catch (e) {
          return Promise.resolve(null);       // NotFoundException etc.: nothing in this frame
        }
      },
    };
  }

  function getEngine() {
    if (engine) return Promise.resolve(engine);
    if (enginePromise) return enginePromise;
    enginePromise = (async function () {
      // Native detector (Chrome on Android): fast and battery friendly.
      if ('BarcodeDetector' in root) {
        try {
          var supported = await root.BarcodeDetector.getSupportedFormats();
          if (supported.indexOf('data_matrix') >= 0 && supported.indexOf('ean_13') >= 0) {
            var det = new root.BarcodeDetector({ formats: NATIVE_FORMATS.filter(function (f) { return supported.indexOf(f) >= 0; }) });
            engine = {
              name: 'native',
              decode: async function (canvas) {
                var r = await det.detect(canvas);
                return r && r.length ? { text: r[0].rawValue, format: r[0].format } : null;
              },
            };
            return engine;
          }
        } catch (e) { /* fall through to ZXing */ }
      }
      engine = zxingEngine(await loadZXing());   // iOS Safari, desktop browsers, ...
      return engine;
    })();
    enginePromise.catch(function () { enginePromise = null; });
    return enginePromise;
  }

  function decodeCanvas(canvas) {
    return getEngine().then(function (eng) { return eng.decode(canvas); }).then(function (r) {
      return r ? parse(r.text, r.format) : null;
    });
  }

  // ---------------------------------------------------------------- live camera
  var stream = null, timer = null, running = false, scratch = null;

  var isLiveSupported = function () {
    return !!(root.isSecureContext && root.navigator && navigator.mediaDevices && navigator.mediaDevices.getUserMedia);
  };

  function stop() {
    running = false;
    if (timer) { clearTimeout(timer); timer = null; }
    if (stream) { stream.getTracks().forEach(function (t) { t.stop(); }); stream = null; }
  }

  async function start(video, opts) {
    opts = opts || {};
    stop();
    if (!isLiveSupported()) throw Object.assign(new Error('insecure'), { name: 'InsecureContext' });

    stream = await navigator.mediaDevices.getUserMedia({
      audio: false,
      video: { facingMode: { ideal: 'environment' }, width: { ideal: 1920 }, height: { ideal: 1080 } },
    });
    var track = stream.getVideoTracks()[0];
    var caps = (track.getCapabilities && track.getCapabilities()) || {};
    try {   // continuous autofocus where the device supports it
      if (caps.focusMode && caps.focusMode.indexOf('continuous') >= 0) {
        await track.applyConstraints({ advanced: [{ focusMode: 'continuous' }] });
      }
    } catch (e) { /* optional */ }

    video.setAttribute('playsinline', '');
    video.muted = true;
    video.srcObject = stream;
    await video.play();

    var eng = await getEngine();
    running = true;
    scratch = scratch || document.createElement('canvas');

    var tick = async function () {
      if (!running) return;
      var t0 = performance.now();
      try {
        if (video.videoWidth > 0) {
          var scale = Math.min(1, 1280 / Math.max(video.videoWidth, video.videoHeight));
          scratch.width = Math.round(video.videoWidth * scale);
          scratch.height = Math.round(video.videoHeight * scale);
          scratch.getContext('2d', { willReadFrequently: true }).drawImage(video, 0, 0, scratch.width, scratch.height);
          var r = await eng.decode(scratch);
          var parsed = r && parse(r.text, r.format);
          if (parsed && running) {
            running = false;                      // one hit per start(); the caller restarts for the next pack
            if (root.navigator && navigator.vibrate) navigator.vibrate(60);
            if (opts.onCode) opts.onCode(parsed);
            return;                              // caller decides whether to keep scanning
          }
        }
      } catch (e) { /* a bad frame is not fatal */ }
      if (running) timer = setTimeout(tick, Math.max(60, 140 - (performance.now() - t0)));
    };
    tick();

    return { torch: !!caps.torch, engine: eng.name };
  }

  async function setTorch(on) {
    var track = stream && stream.getVideoTracks()[0];
    if (!track) return false;
    try { await track.applyConstraints({ advanced: [{ torch: !!on }] }); return true; }
    catch (e) { return false; }
  }

  // ----------------------------------------------------------------- photo mode
  async function loadBitmap(file) {
    if (root.createImageBitmap) {
      try { return await createImageBitmap(file, { imageOrientation: 'from-image' }); } catch (e) { /* fall back */ }
    }
    return new Promise(function (resolve, reject) {
      var url = URL.createObjectURL(file), img = new Image();
      img.onload = function () { URL.revokeObjectURL(url); resolve(img); };
      img.onerror = function () { URL.revokeObjectURL(url); reject(new Error('image')); };
      img.src = url;
    });
  }

  async function decodeFile(file) {
    var bmp = await loadBitmap(file);
    var w0 = bmp.width || bmp.naturalWidth, h0 = bmp.height || bmp.naturalHeight;
    var canvas = document.createElement('canvas');
    // Big phone photos: try a high and a lower resolution, DataMatrix codes decode best at different scales.
    var maxes = [2000, 1200, 800];
    for (var k = 0; k < maxes.length; k++) {
      var scale = Math.min(1, maxes[k] / Math.max(w0, h0));
      canvas.width = Math.round(w0 * scale);
      canvas.height = Math.round(h0 * scale);
      canvas.getContext('2d', { willReadFrequently: true }).drawImage(bmp, 0, 0, canvas.width, canvas.height);
      var res = await decodeCanvas(canvas);
      if (res) { if (bmp.close) bmp.close(); return res; }
    }
    if (bmp.close) bmp.close();
    return null;
  }

  var api = {
    parse: parse, gtinValid: gtinValid, expiryMonth: expiryMonth,
    isLiveSupported: isLiveSupported, start: start, stop: stop, setTorch: setTorch,
    decodeFile: decodeFile, decodeCanvas: decodeCanvas, preload: function () { return getEngine().catch(function () {}); },
  };
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  root.ApoScan = api;
})(typeof window !== 'undefined' ? window : globalThis);
