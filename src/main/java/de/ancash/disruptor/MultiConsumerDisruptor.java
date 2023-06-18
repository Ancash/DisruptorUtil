package de.ancash.disruptor;

import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

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

public class MultiConsumerDisruptor<T> {

	private final Disruptor<T> disruptor;

	@SafeVarargs
	public MultiConsumerDisruptor(EventFactory<T> ef, ProducerType type, EventHandler<T>... handler) {
		this(ef, 1024 * 4, type,
				new PhasedBackoffWaitStrategy(0, 1000, TimeUnit.MICROSECONDS, new SleepingWaitStrategy(0, 1)), handler);
	}

	@SuppressWarnings("unchecked")
	@SafeVarargs
	public MultiConsumerDisruptor(EventFactory<T> ef, int size, ProducerType type, WaitStrategy strat,
			EventHandler<T>... handler) {
		disruptor = new Disruptor<>(ef, size, DaemonThreadFactory.INSTANCE, type, strat);
		disruptor.handleEventsWith(IntStream.range(0, handler.length).boxed()
				.map(i -> new DelegatingEventHandler<T>(handler.length, i, handler[i]))
				.toArray(DelegatingEventHandler[]::new));
		disruptor.start();
	}

	public void publishEvent(EventTranslator<T> et) {
		disruptor.publishEvent(et);
	}

	public <A> void publishEvent(EventTranslatorOneArg<T, A> et, A a) {
		disruptor.publishEvent(et, a);
	}

	public <A, B> void publishEvent(EventTranslatorTwoArg<T, A, B> et, A a, B b) {
		disruptor.publishEvent(et, a, b);
	}

	public <A, B, C> void publishEvent(EventTranslatorThreeArg<T, A, B, C> et, A a, B b, C c) {
		disruptor.publishEvent(et, a, b, c);
	}

//	public static void main(String[] args) throws InterruptedException {
//		int w = 4;
//		MultiConsumerDisruptor<MCDTestEvent> mcd = new MultiConsumerDisruptor<>(MCDTestEvent::new, ProducerType.SINGLE,
//				IntStream.range(0, w).boxed().map(i -> new MCDTestHandler(w, i)).toArray(MCDTestHandler[]::new));
//		while (true) {
//
//			int c = 50000000;
//			counter.set(c);
//			long now = System.currentTimeMillis();
//			for (int i = counter.get(); i > 0; i--) {
//				mcd.publishEvent((event, seq) -> event.stamp = seq % w);
//			}
//			System.out.println("published all");
//			while (counter.get() != 0) {
//				Thread.sleep(10);
//			}
//			System.out.println(System.currentTimeMillis() - now + " ms");
//			System.out.println(c / ((System.currentTimeMillis() - now) / 1000D) + " /sec");
//		}
//	}
//
//	static AtomicInteger counter = new AtomicInteger(10000);
}

class DelegatingEventHandler<T> implements EventHandler<T> {

	final int id, total;
	final EventHandler<T> handler;

	public DelegatingEventHandler(int total, int id, EventHandler<T> handler) {
		this.total = total;
		this.id = id;
		this.handler = handler;
	}

	@Override
	public void onEvent(T event, long sequence, boolean endOfBatch) throws Exception {
		if (sequence % total != id)
			return;
		handler.onEvent(event, sequence, endOfBatch);
	}
}

//class MCDTestHandler implements EventHandler<MCDTestEvent> {
//
//	int id, total;
//
//	public MCDTestHandler(int total, int id) {
//		this.total = total;
//		this.id = id;
//	}
//
//	@Override
//	public void onEvent(MCDTestEvent event, long sequence, boolean endOfBatch) throws Exception {
//		if (sequence % total != id)
//			return;
////		LockSupport.parkNanos(event.stamp);
////		Thread.sleep(10 * event.stamp);
//		MultiConsumerDisruptor.counter.decrementAndGet();
//	}
//
//}

//class MCDTestEvent {
//
//	long stamp;
//
//}