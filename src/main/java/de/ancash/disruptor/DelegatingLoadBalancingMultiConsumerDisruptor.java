package de.ancash.disruptor;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.EventTranslatorThreeArg;
import com.lmax.disruptor.EventTranslatorTwoArg;
import com.lmax.disruptor.PhasedBackoffWaitStrategy;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;

/**
 * only good for events take up some more time, for higher throughput use {@link MultiConsumerDisruptor}
 */
public class DelegatingLoadBalancingMultiConsumerDisruptor<T> {

//	public static void main(String[] args) throws InterruptedException {
//		int w = 4;
//		DelegatingLoadBalancingMultiConsumerDisruptor<LBMCDTestEvent> mcd = new DelegatingLoadBalancingMultiConsumerDisruptor<LBMCDTestEvent>(
//				LBMCDTestEvent::new, ProducerType.SINGLE,
//				IntStream.range(0, w).boxed().map(LBMCDTestHandler::new).toArray(LBMCDTestHandler[]::new));
//		while (true) {
//			counter.set(50000000);
//			long now = System.currentTimeMillis();
//			for (int i = counter.get(); i > 0; i--) {
//				mcd.publishEvent((event, seq) -> event.stamp = seq % w);
//			}
//			System.out.println("published all events");
//			while (counter.get() != 0) {
//				Thread.sleep(10);
//			}
//			System.out.println(System.currentTimeMillis() - now + " ms");
//		}
//	}
//
//	static class LBMCDTestHandler implements EventHandler<LBMCDTestEvent> {
//
//		int id;
//
//		public LBMCDTestHandler(int id) {
//			this.id = id;
//		}
//
//		@Override
//		public void onEvent(LBMCDTestEvent event, long sequence, boolean endOfBatch) throws Exception {
////			Thread.sleep(10 * event.stamp);
//			counter.decrementAndGet();
////			LockSupport.parkNanos(event.stamp);
////			if (counter.decrementAndGet() % 100000 == 0)
////				System.out.println(Thread.currentThread().getName() + " exec " + id + " ");
//		}
//
//	}
//
//	static class LBMCDTestEvent {
//
//		long stamp;
//
//	}
//
//	static AtomicInteger counter = new AtomicInteger(10000000);

	static int delegateSize = 1024 * 2;

	private final Disruptor<EventWrapper<T>> disruptor;
	final EventHandlerWrapper<T>[] ehw;

	@SafeVarargs
	public DelegatingLoadBalancingMultiConsumerDisruptor(EventFactory<T> ef, ProducerType type, EventHandler<T>... handler) {
		this(ef, 1024 * 2, type,
				new PhasedBackoffWaitStrategy(0, 1000, TimeUnit.MICROSECONDS, new SleepingWaitStrategy(0, 1)), handler);
	}

	@SuppressWarnings("unchecked")
	@SafeVarargs
	public DelegatingLoadBalancingMultiConsumerDisruptor(EventFactory<T> ef, int size, ProducerType type, WaitStrategy strat,
			EventHandler<T>... handler) {
		disruptor = new Disruptor<EventWrapper<T>>(() -> new EventWrapper<T>(ef), size, DaemonThreadFactory.INSTANCE,
				type, strat);
		ehw = new EventHandlerWrapper[handler.length];
		for (int i = 0; i < handler.length; i++)
			ehw[i] = new EventHandlerWrapper<T>(this, i, ef, handler[i]);
		disruptor.handleEventsWith(ehw);
		disruptor.start();
	}

	private int nextId() {
		return nextId0(0);
	}
	
	private int nextId0(int iter) {
		int minUsed = Integer.MAX_VALUE;
		int minPos = 0;
		for (int i = 0; i < ehw.length; i++) {
			int t = ehw[i].dehw.used.get();
			if (t < minUsed) {
				minUsed = t;
				minPos = i;
			}
		}
		if (minUsed >= delegateSize) {
			LockSupport.parkNanos(Math.max((long) Math.log(iter), 1));
			return nextId0(iter + 1);
		}
		ehw[minPos].dehw.used.incrementAndGet();
		return minPos;
	}

	public void publishEvent(EventTranslator<T> et) {
		disruptor.publishEvent((e, seq) -> {
			e.id = nextId();
			et.translateTo(e.event, seq);
		});
	}

	public <A> void publishEvent(EventTranslatorOneArg<T, A> et, A aa) {
		disruptor.publishEvent((e, seq, a) -> {
			e.id = nextId();
			et.translateTo(e.event, seq, a);
		}, aa);
	}

	public <A, B> void publishEvent(EventTranslatorTwoArg<T, A, B> et, A aa, B bb) {
		disruptor.publishEvent((e, seq, a, b) -> {
			e.id = nextId();
			et.translateTo(e.event, seq, a, b);
		}, aa, bb);
	}

	public <A, B, C> void publishEvent(EventTranslatorThreeArg<T, A, B, C> et, A aa, B bb, C cc) {
		disruptor.publishEvent((e, seq, a, b, c) -> {
			e.id = nextId();
			et.translateTo(e.event, seq, a, b, c);
		}, aa, bb, cc);
	}
}

class DelegatingEventHandlerWrapper<T> implements EventHandler<EventWrapper<T>> {

	final EventHandler<T> handler;
	AtomicInteger used = new AtomicInteger();

	public DelegatingEventHandlerWrapper(EventHandler<T> handler) {
		this.handler = handler;
	}

	@Override
	public void onEvent(EventWrapper<T> event, long sequence, boolean endOfBatch) throws Exception {
		handler.onEvent(event.event, sequence, endOfBatch);
		used.decrementAndGet();
	}

}

class EventWrapper<T> {

	T event;
	int id;

	EventWrapper(EventFactory<T> ef) {
		event = ef.newInstance();
	}
}

class EventHandlerWrapper<T> implements EventHandler<EventWrapper<T>> {

	final EventHandler<T> handler;
	final int id;
	final SingleConsumerDisruptor<EventWrapper<T>> delegate;
	final DelegatingLoadBalancingMultiConsumerDisruptor<T> lbmcd;
	final DelegatingEventHandlerWrapper<T> dehw;

	public EventHandlerWrapper(DelegatingLoadBalancingMultiConsumerDisruptor<T> lbmcd, int id, EventFactory<T> ef,
			EventHandler<T> handler) {
		dehw = new DelegatingEventHandlerWrapper<T>(handler);
		delegate = new SingleConsumerDisruptor<EventWrapper<T>>(() -> new EventWrapper<T>(ef),
				DelegatingLoadBalancingMultiConsumerDisruptor.delegateSize, ProducerType.SINGLE,
				new PhasedBackoffWaitStrategy(0, 500, TimeUnit.MICROSECONDS, new SleepingWaitStrategy(0, 1)), dehw);
		this.handler = handler;
		this.lbmcd = lbmcd;
		this.id = id;
	}

	@Override
	public void onEvent(EventWrapper<T> event, long sequence, boolean endOfBatch) throws Exception {
		if (event.id != id)
			return;
		delegate.publishEvent((e, seq) -> e.event = event.event);
	}
}