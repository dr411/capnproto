// Copyright (c) 2019 Cloudflare, Inc. and contributors
// Licensed under the MIT License:
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

#include "byte-stream.h"
#include <kj/one-of.h>
#include <kj/debug.h>

namespace capnp {

class ByteStreamFactory::CapnpToKjStreamAdapter final
    : public capnp::ByteStream::Server, private kj::AsyncInputStream {
  // Implemetns Cap'n Proto ByteStream as a wrapper around a KJ stream.
  //
  // Subclasses AsyncInputStream only for path-shortening purposes.

public:
  CapnpToKjStreamAdapter(ByteStreamFactory& factory,
                         kj::Own<kj::AsyncOutputStream> inner,
                         kj::PromiseFulfillerPair<void> paf = kj::newPromiseAndFulfiller<void>())
      : factory(factory),
        inner(kj::mv(inner)),
        readyPromise(paf.promise.fork()),
        readyFulfiller(kj::mv(paf.fulfiller)),
        probeTask(probeForShorterPath()) {}

  class SubstreamCallbackImpl: public capnp::ByteStream::SubstreamCallback::Server {
  public:
    SubstreamCallbackImpl(ByteStreamFactory& factory,
                          kj::Own<kj::AsyncOutputStream> originalStream,
                          kj::Own<kj::PromiseFulfiller<uint64_t>> originalPumpfulfiller,
                          uint64_t originalPumpLimit)
        : factory(factory),
          originalStream(kj::mv(originalStream)),
          originalPumpfulfiller(kj::mv(originalPumpfulfiller)),
          originalPumpLimit(originalPumpLimit) {}

    ~SubstreamCallbackImpl() noexcept(false) {
      if (!done) {
        originalPumpfulfiller->reject(KJ_EXCEPTION(DISCONNECTED,
            "stream disconnected because SubstreamCallbackImpl was never called back"));
      }
    }

    kj::Promise<void> ended(EndedContext context) override {
      KJ_REQUIRE(!done);
      uint64_t actual = context.getParams().getByteCount();
      KJ_REQUIRE(actual <= originalPumpLimit);

      done = true;

      // EOF before pump completed. Signal a short pump.
      originalPumpfulfiller->fulfill(context.getParams().getByteCount());

      #error "how do we unwind the original probe task?"
      #error "for that matter, what happende to the original probe task when the path was shortened???"

      return kj::READY_NOW;
    }

    kj::Promise<void> reachedLimit(ReachedLimitContext context) override {
      KJ_REQUIRE(!done);
      done = true;

      // The full pump completed.
      originalPumpfulfiller->fulfill(kj::cp(originalPumpLimit));

      // Allow the shortened stream to redirect back to our original underlying stream.
      auto results = context.getResults(capnp::MessageSize { 4, 1 });
      results.setNext(factory.streamSet.add(
          kj::heap<CapnpToKjStreamAdapter>(kj::mv(originalStream))));
      return kj::READY_NOW;
    }

  private:
    ByteStreamFactory& factory;
    kj::Own<kj::AsyncOutputStream> originalStream;
    kj::Own<kj::PromiseFulfiller<uint64_t>> originalPumpfulfiller;
    uint64_t originalPumpLimit;
    bool done = false;
  };

  class PathProber: public kj::AsyncInputStream {
  public:
    kj::Promise<uint64_t> pumpToShorterPath(capnp::ByteStream::Client target, uint64_t limit) {
      // If our probe succeeds in finding a KjToCapnpStreamAdapter somewhere down the stack, that
      // will call this method to provide the shortened path.

      auto innerStream = kj::mv(KJ_ASSERT_NONNULL(
          parent.inner.tryGet<kj::Own<kj::AsyncOutputStream>>()));

      auto req = target.getSubstreamRequest();
      req.setLimit(limit);
      auto paf = kj::newPromiseAndFulfiller<uint64_t>();
      req.setCallback(kj::heap<SubstreamCallbackImpl>(parent.factory,
          kj::mv(innerStream), kj::mv(paf.fulfiller), limit));

      parent.inner = req.send().getSubstream();
      readyFulfiller->fulfill();
      #error "what happens to the probe task?? I think we need to pass off the PathProber to the next CapnpToKjStreamAdapter when it appears..."
    }

    kj::Promise<void> probeForShorterPath(kj::AsyncOutputStream& output,
                                          kj::Own<kj::PromiseFulfiller<void>> fulfiller) {
      return kj::evalNow([&]() -> kj::Promise<uint64_t> {
        return pumpTo(output, kj::maxValue);
      }).then([&fulfiller = *fulfiller](uint64_t) {
        // `fulfiller` may have already been fulfilled, but just in case it hasn't, fulfill it
        // now, because we're definitely done probing for shorter paths.
        fulfiller.fulfill();
      }).eagerlyEvaluate([fulfiller = kj::mv(fulfiller)](kj::Exception&& exception) mutable {
        fulfiller->reject(kj::mv(exception));
      });
    }

    kj::Promise<size_t> tryRead(void* buffer, size_t minBytes, size_t maxBytes) override {
      // If this is called, it means the tryPumpFrom() in probeForShorterPath() eventually invoked
      // code that tries to read manually from the source. We don't know what this code is doing
      // exactly, but we do know for sure that the endpoint is not a KjToCapnpStreamAdapter, so
      // we can't optimize. Instead, we pretend that we immediately hit EOF, ending the pump. This
      // works because pumps do not propagate EOF -- the destination can still receive further
      // writes and pumps. Basically our probing pump becomes a no-op.
      return size_t(0);
    }

    kj::Promise<uint64_t> pumpTo(kj::AsyncOutputStream& output, uint64_t amount) override {
      // Call the stream's `tryPumpFrom()` as a way to discover where the data will eventually go,
      // in hopes that we find we can shorten the path.
      KJ_IF_MAYBE(promise, output.tryPumpFrom(*this)) {
        // tryPumpFrom() returned non-null. Either it called `tryRead()` or `pumpTo()` (see
        // below), or it plans to do so in the future.
        return kj::mv(*promise);
      } else {
        // There is no shorter path.
        return uint64_t(0);
      }
    }

  private:
    kj::Maybe<CapnpToKjStreamAdapter&> parent;
  };

  kj::Maybe<kj::Promise<Capability::Client>> shortenPath() override {
    return readyPromise.addBranch()
        .then([this]() -> kj::Promise<Capability::Client> {
      KJ_IF_MAYBE(cap, inner.tryGet<capnp::ByteStream::Client>()) {
        return Capability::Client(*cap);
      } else {
        // We can't shorten.
        return kj::NEVER_DONE;
      }
    });
  }

  kj::Promise<void> write(WriteContext context) override {
    return probeTask.addBranch().then([this, context]() mutable {
      auto& stream = *KJ_REQUIRE_NONNULL(
          requestState->assertState<kj::Own<kj::AsyncOutputStream>>(),
          "already called end()");

      auto data = context.getParams().getBytes();

      // TODO(now): Deal with concurrent writes, or add streaming to Cap'n Proto.
      return requestState->canceler.wrap(stream.write(data.begin(), data.size()));
    });
  }

  kj::Promise<void> end(EndContext context) override {
    return probeTask.addBranch().then([this, context]() mutable {
      KJ_REQUIRE_NONNULL(
          requestState->assertState<kj::Own<kj::AsyncOutputStream>>(),
          "already called end()");
      requestState->state = RequestState::Done();
      KJ_IF_MAYBE(f, requestState->doneFulfiller) {
        f->fulfill();
      }
    });
  }

  void directEnd() {
    // Called by KjToCapnpStreamAdapter when path is shortened.

    if (requestState->state.is<kj::Own<kj::AsyncOutputStream>>()) {
      probeTask = nullptr;
      requestState->state = RequestState::Done();
      KJ_IF_MAYBE(f, requestState->doneFulfiller) {
        f->fulfill();
      }
    }
  }

  kj::Promise<void> write(const void* buffer, size_t size) {
    // Called by KjToCapnpStreamAdapter when path is shortened.

    return probeTask.addBranch().then([this, buffer, size]() mutable {
      auto& stream = *KJ_REQUIRE_NONNULL(
          requestState->assertState<kj::Own<kj::AsyncOutputStream>>(),
          "already called end()");

      return requestState->canceler.wrap(stream.write(buffer, size));
    });
  }

  kj::Promise<void> write(kj::ArrayPtr<const kj::ArrayPtr<const byte>> pieces) {
    // Called by KjToCapnpStreamAdapter when path is shortened.

    return probeTask.addBranch().then([this, pieces]() mutable {
      auto& stream = *KJ_REQUIRE_NONNULL(
          requestState->assertState<kj::Own<kj::AsyncOutputStream>>(),
          "already called end()");

      return requestState->canceler.wrap(stream.write(pieces));
    });
  }

  kj::Promise<uint64_t> pumpFrom(kj::AsyncInputStream& input, uint64_t amount) {
    // Called by KjToCapnpStreamAdapter when path is shortened.

    return probeTask.addBranch().then([this, &input, amount]() mutable {
      auto& stream = *KJ_REQUIRE_NONNULL(
          requestState->assertState<kj::Own<kj::AsyncOutputStream>>(),
          "already called end()");

      return requestState->canceler.wrap(input.pumpTo(stream, amount));
    });
  }

private:
  ByteStreamFactory& factory;

  kj::OneOf<kj::Own<kj::AsyncOutputStream>, capnp::ByteStream::Client> inner;

  kj::ForkedPromise<void> readyPromise;
  kj::Own<kj::PromiseFulfiller<void>> readyFulfiller;
  // Resolves / is fulfilled when the value of `inner` is settled and will not change again, at
  // which point it's safe to forward writes to it.

  kj::Promise<void> probeTask;
  // Task which searches for a path and executes the path-shortening. This is not the same as
  // `readyPromise` because `probeTask` may not resolve until the shortened-path pump is actually
  // complete.
};

// =======================================================================================

class ByteStreamFactory::KjToCapnpStreamAdapter final: public kj::AsyncOutputStream {
public:
  KjToCapnpStreamAdapter(ByteStreamFactory& factory, capnp::ByteStream::Client innerParam)
      : inner(kj::mv(innerParam)),
        // If the capnp stream turns out to resolve back to this process, shorten the path.
        // Also, implement whenWriteDisconnected() based on this.
        resolveTask(factory.streamSet.getLocalServer(inner)
            .then([this](kj::Maybe<capnp::ByteStream::Server&> server) -> kj::Promise<void> {
          KJ_IF_MAYBE(s, server) {
            // Yay, we discovered that the ByteStream actually points back to a local KJ stream.
            // We can use this to shorten the path by skipping the RPC machinery.
            auto& o = kj::downcast<CapnpToKjStreamAdapter>(*s);
            optimized = o;
            return o.whenWriteDisconnected();
          } else {
            // The capability is fully-resolved. This suggests that the remote implementation is
            // NOT a CapnpToKjStreamAdapter at all, because CapnpToKjStreamAdapter is designed to
            // always look like a promise. It's some other implementation that doesn't present
            // itself as a promise. We have no way to detect when it is disconnected.
            return kj::NEVER_DONE;
          }
        }, [](kj::Exception&& e) -> kj::Promise<void> {
          // getLocalServer() thrown when the capability is a promise cap that rejects. We can
          // use this to implement whenWriteDisconnected().
          return kj::READY_NOW;
        }).fork()) {}

  ~KjToCapnpStreamAdapter() noexcept(false) {
    // HACK: KJ streams are implicitly ended on destruction, but the RPC stream needs a call. We
    //   use a detached promise for now, which is probably OK since capabilities are refcounted and
    //   asynchronously destroyed anyway.
    // TODO(cleanup): Fix this when KJ streads add an explicit end() method.
    KJ_IF_MAYBE(o, optimized) {
      o->directEnd();
    } else {
      inner.endRequest().send().detach([](kj::Exception&&){});
    }
  }

  capnp::ByteStream::Client getInner() { return inner; }

  kj::Promise<void> write(const void* buffer, size_t size) override {
    KJ_IF_MAYBE(o, optimized) {
      return o->write(buffer, size);
    }

    auto req = inner.writeRequest(MessageSize { 8 + size / sizeof(word), 0 });
    req.setBytes(kj::arrayPtr(reinterpret_cast<const byte*>(buffer), size));
    return req.send().ignoreResult();
  }

  kj::Promise<void> write(kj::ArrayPtr<const kj::ArrayPtr<const byte>> pieces) override {
    KJ_IF_MAYBE(o, optimized) {
      return o->write(pieces);
    }

    size_t size = 0;
    for (auto& piece: pieces) size += piece.size();
    auto req = inner.writeRequest(MessageSize { 8 + size / sizeof(word), 0 });

    auto out = req.initBytes(size);
    byte* ptr = out.begin();
    for (auto& piece: pieces) {
      memcpy(ptr, piece.begin(), piece.size());
      ptr += piece.size();
    }
    KJ_ASSERT(ptr == out.end());

    return req.send().ignoreResult();
  }

  kj::Maybe<kj::Promise<uint64_t>> tryPumpFrom(
      kj::AsyncInputStream& input, uint64_t amount = kj::maxValue) override {
    return pumpLoop(input, 0, amount);
  }

  kj::Promise<void> whenWriteDisconnected() override {
    return resolveTask.addBranch();
  }

private:
  capnp::ByteStream::Client inner;
  kj::Maybe<CapnpToKjStreamAdapter&> optimized;

  kj::ForkedPromise<void> resolveTask;
  // This serves two purposes:
  // 1. Waits for the capability to resolve (if it is a promise), and then shortens the path if
  //    possible.
  // 2. Implements whenWriteDisconnected().

  struct WriteRequestAndBuffer {
    // The order of construction/destruction of lambda captures is unspecified, but we care about
    // ordering between these two things that we want to capture, so... we need a struct.

    Request<capnp::ByteStream::WriteParams, capnp::ByteStream::WriteResults> request;
    Orphan<Data> buffer;  // points into `request`...
  };

  kj::Promise<uint64_t> pumpLoop(kj::AsyncInputStream& input,
                                 uint64_t completed, uint64_t remaining) {
    if (remaining == 0) return completed;

    KJ_IF_MAYBE(o, optimized) {
      // Oh hell yes, this capability actually points back to a stream in our own thread. We can
      // stop sending RPCs and just pump directly.
      auto promise = o->pumpFrom(input, remaining);
      if (completed > 0) {
        promise = promise.then([completed](uint64_t amount) { return amount + completed; });
      }
      return promise;
    } else KJ_IF_MAYBE(rpc, kj::dynamicDowncastIfAvailable<CapnpToKjStreamAdapter::PathProber>(input)) {
      #error "don't shorten if `remaining` is too small to be worth it"
      // Oh interesting, it turns we're hosting an incoming ByteStream which is pumping to this
      // outgoing ByteStream. We can let the Cap'n Proto RPC layer know that it can shorten the
      // path from one to the other.
      return rpc->pumpToShorterPath(inner, remaining)
          .then([completed](uint64_t actual) {
        return completed + actual;
      });
    } else {
      // Pumping from some other kind of steram. Optimize the pump by reading from the input
      // directly into outgoing RPC messages.
      size_t size = kj::min(remaining, 8192);
      auto req = inner.writeRequest(MessageSize { 8 + size / sizeof(word) });

      auto orphanage = Orphanage::getForMessageContaining(
          capnp::ByteStream::WriteParams::Builder(req));

      auto buffer = orphanage.newOrphan<Data>(size);

      WriteRequestAndBuffer wrab = { kj::mv(req), kj::mv(buffer) };

      return input.tryRead(wrab.buffer.get().begin(), 1, size)
          .then([this, &input, completed, remaining, size, wrab = kj::mv(wrab)]
                (size_t actual) mutable -> kj::Promise<uint64_t> {
        if (actual == 0) {
          return completed;
        } if (actual < size) {
          wrab.buffer.truncate(actual);
        }

        wrab.request.adoptBytes(kj::mv(wrab.buffer));
        return wrab.request.send().ignoreResult().then([this, &input, completed, remaining, actual]() {
          return pumpLoop(input, completed + actual, remaining - actual);
        });
      });
    }
  }
};

}  // namespace capnp
