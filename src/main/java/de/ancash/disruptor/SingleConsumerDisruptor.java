package de.ancash.disruptor;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

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

public class SingleConsumerDisruptor<T> {

//	static volatile int counter = 0;
//	static volatile long timer = 0;
//	static volatile int s = 50000000;
//	static int w = 1;
//	static SingleConsumerDisruptor<LongEvent> scd = new SingleConsumerDisruptor<LongEvent>(LongEvent::new, 1024 * 2,
//			ProducerType.SINGLE,
//			new PhasedBackoffWaitStrategy(0, 1000, TimeUnit.MICROSECONDS, new SleepingWaitStrategy(100, 1)),
//			new LongEventHandler());
//	public static void main(String[] args) throws Exception {
//		
//		long now = System.currentTimeMillis();
//		for (int i = 0; i < s; i++) {
//			int o = i;
//			long l = System.nanoTime();
//			scd.publishEvent((event, sequence) -> event.set(l, o));
//		}
//
//		while (counter < s) {
//			Thread.sleep(10);
//		}
//		System.out.println(System.currentTimeMillis() - now + "ms");
//		System.out.println(timer / (double) counter + " ns avg");
//		counter = 0;
//		timer = 0;
//		main(null);
//	}

	final Disruptor<T> disruptor;

	public SingleConsumerDisruptor(EventFactory<T> ef, ProducerType type, EventHandler<T> handler) {
		this(ef, 1024 * 2, type,
				new PhasedBackoffWaitStrategy(0, 1000, TimeUnit.MICROSECONDS, new SleepingWaitStrategy(0, 1)), handler);
	}

	public SingleConsumerDisruptor(EventFactory<T> ef, int size, ProducerType type, WaitStrategy strat,
			EventHandler<T> handler) {
		disruptor = new Disruptor<>(ef, size, DaemonThreadFactory.INSTANCE, type, strat);
		new ThreadFactory() {
			
			@Override
			public Thread newThread(Runnable arg0) {
				// TODO Auto-generated method stub
				return null;
			}
		};
		disruptor.handleEventsWith(handler);
		disruptor.start();
	}
	
	public void stop() {
		disruptor.halt();
	}

	public boolean hasCapacity(int i) {
		return disruptor.getRingBuffer().hasAvailableCapacity(i);
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
}

//class LongEvent {
//	long value;
//	long iter;
//
//	public LongEvent set(long value, long iter) {
//		this.value = value;
//		this.iter = iter;
//		return this;
//	}
//
//	@Override
//	public String toString() {
//		return "LE{" + value + ";" + iter + "}";
//	}
//
//}
//
//class LongEventFactory implements EventFactory<LongEvent> {
//
//	public LongEvent newInstance() {
//		return new LongEvent();
//	}
//
//}
//
//class LongEventHandler implements EventHandler<LongEvent> {
//
//	@Override
//	public void onEvent(LongEvent arg0, long arg1, boolean arg2) throws Exception {
//		long l = SingleConsumerDisruptor.counter++;
//		SingleConsumerDisruptor.timer += (System.nanoTime() - arg0.value);
//		if (l % 1000000 == 0)
//			System.out.println((double) l / SingleConsumerDisruptor.s * 100D + "%");
//	}
//}