// The CSP allows no eval. zod probes for it with `new Function` when it builds an object schema,
// and the webview reports a CSP violation even though zod catches the error. jitless mode skips
// the probe, but only for schemas built after it is set, and modules run before the code of the
// module that imports them: so this is a module of its own, imported before any other.
import { config } from 'zod';

config({ jitless: true });
