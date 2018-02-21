package io.objectbox.performanceapp.paperdb;

import android.content.Context;
import android.os.Environment;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.objectbox.Property;
import io.objectbox.performanceapp.PerfTest;
import io.objectbox.performanceapp.PerfTestRunner;
import io.objectbox.performanceapp.TestType;
import io.objectbox.performanceapp.objectbox.SimpleEntity_;
import io.paperdb.Book;
import io.paperdb.Paper;
import io.reactivex.Flowable;
import io.reactivex.Observable;
import io.reactivex.Scheduler;
import io.reactivex.internal.operators.parallel.ParallelDoOnNextTry;
import io.reactivex.internal.operators.parallel.ParallelFlatMap;
import io.reactivex.parallel.ParallelFlowable;
import io.reactivex.schedulers.Schedulers;

import static java.util.stream.Collectors.toList;

public class PaperDBPerfTest extends PerfTest {
    private final String ENTITIES_KEY = "entities";
    private final String ROOT_STORAGE_PATH = "/paperdb/";
    private final String BOOK_NAME = "entities";

    private Book store;
    private boolean versionLoggedOnce;

    @Override
    public String name() {
        return "Paper DB";
    }

    public void setUp(Context context, PerfTestRunner testRunner) {
        super.setUp(context, testRunner);
        Paper.init(context);
        String path = Environment.getExternalStorageDirectory().getAbsolutePath().concat(ROOT_STORAGE_PATH);
        store = Paper.bookOn(path, BOOK_NAME);

        if (!versionLoggedOnce) {
            log("Paper DB 2.6 (Kryo 4.0.1)");
            versionLoggedOnce = true;
        }
    }

    @Override
    public void run(TestType type) {
        switch (type.name) {
            case TestType.CRUD:
                runBatchPerfTest(false);
                break;
            case TestType.CRUD_SCALARS:
                runBatchPerfTest(true);
                break;
            case TestType.CRUD_INDEXED:
                runBatchPerfTestBatch();
                break;
            case TestType.QUERY_STRING:
                runQueryByString();
                break;
            case TestType.QUERY_STRING_INDEXED:
                runQueryByStringBatch();
                break;
            case TestType.QUERY_INTEGER:
                runQueryByInteger();
                break;
            case TestType.QUERY_INTEGER_INDEXED:
                runQueryByIntegerBatch();
                break;
            case TestType.QUERY_ID:
                runQueryById(false);
                break;
            case TestType.QUERY_ID_RANDOM:
                runQueryById(true);
                break;
        }
    }

    public void runBatchPerfTest(boolean scalarsOnly) {
        List<SimpleEntity> entities = prepareAndPutEntities(scalarsOnly);

        for (SimpleEntity entity : entities) {
            if (scalarsOnly) {
                setRandomScalars(entity);
            } else {
                setRandomValues(entity);
            }
        }

        startBenchmark("update");
        Flowable.fromIterable(entities)
                .flatMap(entity -> Flowable.just(entity)
                        .subscribeOn(Schedulers.io())
                        .doOnNext(e -> store.write(String.valueOf(e.getId()), e)))
                .blockingSubscribe();
        stopBenchmark();

        entities = null;
        List<SimpleEntity> updatedEntities = new ArrayList<>();

        startBenchmark("load");
        Flowable.fromIterable(store.getAllKeys())
                .flatMap(key -> Flowable.just(key)
                        .subscribeOn(Schedulers.io())
                        .map(k -> (SimpleEntity) store.read(k)))
                .blockingSubscribe(updatedEntities::add);
        stopBenchmark();

        assertEntityCount(updatedEntities.size());

        startBenchmark("access");
        accessAll(updatedEntities);
        stopBenchmark();

        startBenchmark("delete");
        Flowable.fromIterable(store.getAllKeys())
                .flatMap(key -> Flowable.just(key)
                        .subscribeOn(Schedulers.io())
                        .doOnNext(k -> store.delete(k)))
                .blockingSubscribe();
        stopBenchmark();

        assertEntityCountZero(store.getAllKeys().size());
    }

    public void runBatchPerfTestBatch() {
        List<SimpleEntity> entities = prepareAndPutEntitiesBatch();

        for (SimpleEntity entity : entities) {
            setRandomValues(entity);
        }
        startBenchmark("update");
        store.write(ENTITIES_KEY, entities);
        stopBenchmark();

        entities = null;

        startBenchmark("load");
        List<SimpleEntity> updatedEntities = store.read(ENTITIES_KEY, new ArrayList<SimpleEntity>());
        stopBenchmark();

        startBenchmark("access");
        accessAll(updatedEntities);
        stopBenchmark();

        startBenchmark("delete");
        store.delete(ENTITIES_KEY);
        stopBenchmark();

        assertEntityCountZero(store.getAllKeys().size());
    }

    protected void setRandomValues(SimpleEntity entity) {
        setRandomScalars(entity);
        entity.setSimpleString(randomString());
        entity.setSimpleByteArray(randomBytes());
    }

    private void setRandomScalars(SimpleEntity entity) {
        entity.setSimpleBoolean(random.nextBoolean());
        entity.setSimpleByte((byte) random.nextInt());
        entity.setSimpleShort((short) random.nextInt());
        entity.setSimpleInt(random.nextInt());
        entity.setSimpleLong(random.nextLong());
        entity.setSimpleDouble(random.nextDouble());
        entity.setSimpleFloat(random.nextFloat());
    }

    public SimpleEntity createEntity(long id, boolean scalarsOnly) {
        SimpleEntity entity = new SimpleEntity(id);
        if (scalarsOnly) {
            setRandomScalars(entity);
        } else {
            setRandomValues(entity);
        }
        return entity;
    }

    protected void accessAll(List<SimpleEntity> list) {
        for (SimpleEntity entity : list) {
            entity.getId();
            entity.getSimpleBoolean();
            entity.getSimpleByte();
            entity.getSimpleShort();
            entity.getSimpleInt();
            entity.getSimpleLong();
            entity.getSimpleFloat();
            entity.getSimpleDouble();
            entity.getSimpleString();
            entity.getSimpleByteArray();
        }
    }

    private void runQueryByString() {
        if (numberEntities > 10000) {
            log("Reduce number of entities to 10000 to avoid extremely long test runs");
            return;
        }

        List<SimpleEntity> entities = prepareAndPutEntities(false);
        final List<String> stringsToLookup = new ArrayList<>();
        for (int i = 0; i < numberEntities; i++) {
            String text = "";
            while (text.length() < 2) {
                text = entities.get(random.nextInt(numberEntities)).getSimpleString();
            }
            stringsToLookup.add(text);
        }


        List<SimpleEntity> queriedEntities = new ArrayList<>();
        startBenchmark("query");
        long entitiesFound = 0;

        Flowable.fromIterable(store.getAllKeys())
                .flatMap(key -> Flowable.just(key)
                        .subscribeOn(Schedulers.io())
                        .map(k -> (SimpleEntity) store.read(k)))
                .blockingSubscribe(queriedEntities::add);

        for (int i = 0; i < numberEntities; i++) {
            for (SimpleEntity entity : queriedEntities) {
                if (entity.getSimpleString().equals(stringsToLookup.get(i))) {
                    entitiesFound++;
                }
            }
        }
        
        stopBenchmark();
        log("Entities found: " + entitiesFound);
    }

    private void runQueryByStringBatch() {
        if (numberEntities > 10000) {
            log("Reduce number of entities to 10000 to avoid extremely long test runs");
            return;
        }

        List<SimpleEntity> entities = prepareAndPutEntitiesBatch();
        final List<String> stringsToLookup = new ArrayList<>();
        for (int i = 0; i < numberEntities; i++) {
            String text = "";
            while (text.length() < 2) {
                text = entities.get(random.nextInt(numberEntities)).getSimpleString();
            }
            stringsToLookup.add(text);
        }

        startBenchmark("query");
        long entitiesFound = 0;
        List<SimpleEntity> queriedEntities = store.read(ENTITIES_KEY, new ArrayList<SimpleEntity>());
        for (int i = 0; i < numberEntities; i++) {
            for (SimpleEntity entity : queriedEntities) {
                if (entity.getSimpleString().equals(stringsToLookup.get(i))) {
                    entitiesFound++;
                }
            }
        }
        stopBenchmark();
        log("Entities found: " + entitiesFound);
    }

    private void runQueryByInteger() {
        if (numberEntities > 10000) {
            log("Reduce number of entities to 10000 to avoid extremely long test runs");
            return;
        }

        List<SimpleEntity> entities = prepareAndPutEntities(false);
        final int[] valuesToLookup = new int[numberEntities];
        for (int i = 0; i < numberEntities; i++) {
            valuesToLookup[i] = entities.get(random.nextInt(numberEntities)).getSimpleInt();
        }

        List<SimpleEntity> queriedEntities = new ArrayList<>();
        startBenchmark("query");

        long entitiesFound = 0;
        Flowable.fromIterable(store.getAllKeys())
                .flatMap(key -> Flowable.just(key)
                        .subscribeOn(Schedulers.io())
                        .map(k -> (SimpleEntity) store.read(k)))
                .blockingSubscribe(queriedEntities::add);

        for (int i = 0; i < numberEntities; i++) {
            for (SimpleEntity entity : queriedEntities) {
                if (entity.getSimpleInt() == valuesToLookup[i]) {
                    entitiesFound++;
                }
            }
        }

        stopBenchmark();
        log("Entities found: " + entitiesFound);
        assertGreaterOrEqualToNumberOfEntities(entitiesFound);
    }

    private void runQueryByIntegerBatch() {
        if (numberEntities > 10000) {
            log("Reduce number of entities to 10000 to avoid extremely long test runs");
            return;
        }

        List<SimpleEntity> entities = prepareAndPutEntities(false);
        final int[] valuesToLookup = new int[numberEntities];
        for (int i = 0; i < numberEntities; i++) {
            valuesToLookup[i] = entities.get(random.nextInt(numberEntities)).getSimpleInt();
        }

        startBenchmark("query");

        long entitiesFound = 0;
        List<SimpleEntity> entityList = store.read(ENTITIES_KEY, new ArrayList<SimpleEntity>());
        for (int i = 0; i < numberEntities; i++) {
            for (SimpleEntity entity : entityList) {
                if (entity.getSimpleInt() == valuesToLookup[i]) {
                    entitiesFound++;
                }
            }
        }

        stopBenchmark();
        log("Entities found: " + entitiesFound);
    }

    private List<SimpleEntity> prepareAndPutEntities(boolean scalarsOnly) {
        final List<SimpleEntity> entities = new ArrayList<>(numberEntities);
        for (long i = 0; i < numberEntities; i++) {
            entities.add(createEntity(i, scalarsOnly));
        }

        startBenchmark("insert");
        Flowable.fromIterable(entities)
                .flatMap(entity -> Flowable.just(entity)
                        .subscribeOn(Schedulers.io())
                        .doOnNext(e -> store.write(String.valueOf(e.getId()), e)))
                .blockingSubscribe();
        stopBenchmark();

        assertEntityCount(store.getAllKeys().size());
        return entities;
    }

    private List<SimpleEntity> prepareAndPutEntitiesBatch() {
        List<SimpleEntity> entities = new ArrayList<>(numberEntities);
        for (long i = 0; i < numberEntities; i++) {
            entities.add(createEntity(i, false));
        }

        startBenchmark("insert");
        store.write(ENTITIES_KEY, entities);
        stopBenchmark();

        return entities;
    }

    private void runQueryById(boolean randomIds) {
        prepareAndPutEntities(false);
        List<String> ids = Flowable.rangeLong(0, numberEntities)
                .map(String::valueOf).toList().blockingGet();
        if (randomIds) {
            Collections.shuffle(ids);
        }

        startBenchmark("query");
        Flowable.fromIterable(ids)
                .flatMap(id -> Flowable.just(id)
                        .subscribeOn(Schedulers.io())
                        .map(key -> (SimpleEntity) store.read(key)))
                .blockingSubscribe();
        stopBenchmark();
    }

    @Override
    public void tearDown() {
        store.destroy();
    }
}