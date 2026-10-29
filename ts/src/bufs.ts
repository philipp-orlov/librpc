// Copyright (c) 2026 Philipp Orlov

// The libbufs runtime (reader and builder) lives in libbufs/ts; the RPC client and the code rpcc generates import it
// through here so that `@librpc/client` is one module.
export * from '../../../libbufs/ts/src/bufs';
