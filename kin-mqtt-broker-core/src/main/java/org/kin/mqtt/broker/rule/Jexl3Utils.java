package org.kin.mqtt.broker.rule;

import org.apache.commons.jexl3.*;
import org.kin.framework.utils.JSON;
import org.kin.mqtt.broker.bridge.BridgeAttrNames;
import org.kin.mqtt.broker.utils.TopicUtils;

import java.util.function.Consumer;

/**
 * jexl3表达式引擎工具类
 *
 * @author huangjianqin
 * @date 2022/11/21
 */
public final class Jexl3Utils {
    /** 脚本/表达式引擎, 即类code, 比如a > b, 然后{@link MapContext}填入a和b, 就可以实现ab比较了; 脚本和表达式的区别在于脚本支持使用更复杂的语法, 比如定义变量, if, for等等 */
    private static final JexlEngine SCRIPT_ENGINE = new JexlBuilder().create();
    /** 模板引擎, 即类似sql参数填充, 比如"${a} World", 然后{@link MapContext}填入a=Hello, 那么返回Hello World */
    private static final JxltEngine TEMPLATE_ENGINE = SCRIPT_ENGINE.createJxltEngine();

    private Jexl3Utils() {
    }

    /**
     * 执行脚本
     * <p>
     * !!!注意, 脚本执行将会带有3个context
     * 1. {@link  RuleChainContext} 用于修改规则链上下文属性
     * 2. {@link  JSON} 用于json处理
     * 3. {@link  TopicUtils}   用于转换topic匹配正则表达式
     *
     * @param scriptStr 脚本
     * @param consumer  暴露给外部设置脚本参数
     * @return Object 返回值
     */
    public static Object execScript(String scriptStr, Consumer<MapContext> consumer) {
        JexlScript script = SCRIPT_ENGINE.createScript(scriptStr);
        MapContext context = new MapContext();
        consumer.accept(context);
        context.set(JSON.class.getSimpleName(), JSON.class);
        context.set(TopicUtils.class.getSimpleName(), TopicUtils.class);
        context.set(RuleChainAttrNames.class.getSimpleName(), RuleChainAttrNames.class);
        context.set(BridgeAttrNames.class.getSimpleName(), BridgeAttrNames.class);
        return script.execute(context);
    }

    /**
     * 执行表达式
     *
     * @param expressionStr 表达式
     * @param consumer      暴露给外部设置表达式参数
     * @return Object 返回值
     */
    public static Object execExpression(String expressionStr, Consumer<MapContext> consumer) {
        JexlExpression expression = SCRIPT_ENGINE.createExpression(expressionStr);
        MapContext context = new MapContext();
        consumer.accept(context);
        return expression.evaluate(context);
    }

    /**
     * 执行表达式模版
     *
     * @param script   模版
     * @param consumer 暴露给外部设置模版参数
     * @return Object 返回值
     */
    public static Object execExpressionTemplate(String script, Consumer<MapContext> consumer) {
        JxltEngine.Expression expression = TEMPLATE_ENGINE.createExpression(script);
        MapContext context = new MapContext();
        consumer.accept(context);
        return expression.evaluate(context);
    }
}
