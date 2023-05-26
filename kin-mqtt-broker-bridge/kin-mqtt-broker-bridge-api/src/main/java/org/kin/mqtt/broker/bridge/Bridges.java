package org.kin.mqtt.broker.bridge;

import org.kin.framework.collection.CopyOnWriteMap;
import org.kin.framework.utils.ClassUtils;
import org.kin.framework.utils.ExtensionLoader;
import org.kin.mqtt.broker.bridge.definition.BridgeDefinition;
import org.kin.mqtt.broker.core.Type;

import java.util.*;

/**
 * {@link BridgeFactory}工具类
 * @author huangjianqin
 * @date 2023/5/26
 */
public class Bridges {
    private Bridges() {
    }

    /** 关联{@link BridgeDefinition}和{@link BridgeFactory} */
    private static final Map<Class<? extends BridgeDefinition>, BridgeFactory<? extends BridgeDefinition, ? extends Bridge>> DEFINITION_2_FACTORY = new CopyOnWriteMap<>();
    /**
     * 关联bridge type name和{@link BridgeDefinition}
     * key -> bridge type name, value -> {@link BridgeDefinition}
     */
    private static final Map<String, Class<? extends BridgeDefinition>> NAME_2_DEFINITION = new CopyOnWriteMap<>();

    static {
        @SuppressWarnings("rawtypes")
        List<BridgeFactory> factories = ExtensionLoader.getExtensions(BridgeFactory.class);
        registerBridgeFactories(factories);
    }

    /**
     * 创建{@link Bridge}实例
     *
     * @return {@link Bridge}实例
     */
    @SuppressWarnings("unchecked")
    public static Bridge createBridge(BridgeDefinition bridgeDefinition) {
        Class<? extends BridgeDefinition> type = bridgeDefinition.getClass();
        BridgeFactory<BridgeDefinition, Bridge> factory = (BridgeFactory<BridgeDefinition, Bridge>) DEFINITION_2_FACTORY.get(type);
        if (Objects.isNull(factory)) {
            throw new IllegalArgumentException(String.format("bridge definition '%s' is not support", type.getName()));
        }

        return factory.create(bridgeDefinition);
    }

    /**
     * 根据bridge type获取{@link BridgeDefinition}类型
     * @return {@link BridgeDefinition}类型
     */
    public static Class<? extends BridgeDefinition> getDefinitionClassByName(String typeName){
        Class<? extends BridgeDefinition> bdClass = NAME_2_DEFINITION.get(typeName.toLowerCase());
        if (bdClass == null) {
            throw new IllegalArgumentException(String.format("can not find bridge definition class associated with " + typeName));
        }
        return bdClass;
    }

    /**
     * 注册{@link Bridge}实现
     * 通过{@link  BridgeFactory}实现类泛型获取{@link BridgeDefinition}实现类信息, 注意, 此处{@link  BridgeFactory}实现类不能是匿名内部类和lambda
     * 自定义{@link Bridge}实现, 可通过该方法注册自定义{@link Bridge}构建逻辑
     *
     * @param factories {@link Bridge}实现构造逻辑
     */
    @SuppressWarnings("rawtypes")
    public static void registerBridgeFactories(BridgeFactory... factories) {
        registerBridgeFactories(Arrays.asList(factories));
    }

    /**
     * 注册{@link Bridge}实现
     * 通过{@link  BridgeFactory}实现类泛型获取{@link BridgeDefinition}实现类信息, 注意, 此处{@link  BridgeFactory}实现类不能是匿名内部类和lambda
     * 自定义{@link Bridge}实现, 可通过该方法注册自定义{@link Bridge}构建逻辑
     *
     * @param factories {@link Bridge}实现构造逻辑
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static void registerBridgeFactories(Collection<BridgeFactory> factories) {
        Map<Class<? extends BridgeDefinition>, BridgeFactory<? extends BridgeDefinition, ? extends Bridge>> definition2Factory = new HashMap<>();
        Map<String, Class<? extends BridgeDefinition>> name2definition = new HashMap<>();
        for (BridgeFactory<? extends BridgeDefinition, ? extends Bridge> factory : factories) {
            List<Class<?>> genericTypes = ClassUtils.getSuperInterfacesGenericRawTypes(BridgeFactory.class, factory.getClass());
            Class<? extends BridgeDefinition> bdClass = (Class<? extends BridgeDefinition>) genericTypes.get(0);

            if (DEFINITION_2_FACTORY.containsKey(bdClass)) {
                throw new IllegalStateException(String.format("bridge with '%s' definition has registered", bdClass.getName()));
            }

            Type typeAnno = bdClass.getAnnotation(Type.class);
            if (typeAnno == null) {
                throw new IllegalArgumentException("can not find Type annotation on bridge definition class");
            }

            name2definition.put(typeAnno.value(), bdClass);
            definition2Factory.put(bdClass, factory);
        }

        NAME_2_DEFINITION.putAll(name2definition);
        DEFINITION_2_FACTORY.putAll(definition2Factory);
    }

    /**
     * 注册{@link Bridge}实现
     * 自定义{@link Bridge}实现, 可通过该方法注册自定义{@link Bridge}构建逻辑
     *
     * @param bdClass bridge定义class
     * @param factory {@link Bridge}实现构造逻辑
     */
    public static void registerBridgeFactory(Class<? extends BridgeDefinition> bdClass, BridgeFactory<? extends BridgeDefinition, ? extends Bridge> factory) {
        if (DEFINITION_2_FACTORY.containsKey(bdClass)) {
            throw new IllegalStateException(String.format("bridge with '%s' definition has registered", bdClass.getName()));
        }

        Type typeAnno = bdClass.getAnnotation(Type.class);
        if (typeAnno == null) {
            throw new IllegalArgumentException("can not find Type annotation on bridge definition class");
        }

        NAME_2_DEFINITION.put(typeAnno.value().toLowerCase(), bdClass);
        DEFINITION_2_FACTORY.put(bdClass, factory);
    }
}
