package com.wrmsr.presto.util;

import org.apache.bval.jsr303.ApacheValidationProvider;

import javax.validation.Validator;

import java.util.List;

import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableList;

public class Validation
{
    private static final Validator VALIDATOR = javax.validation.Validation.byProvider(ApacheValidationProvider.class).configure().buildValidatorFactory().getValidator();

    public static class Violation
    {
        private final String propertyPath;
        private final String message;

        public Violation(String propertyPath, String message)
        {
            this.propertyPath = propertyPath;
            this.message = message;
        }

        @Override
        public String toString()
        {
            return "Violation{" +
                    "propertyPath='" + propertyPath + '\'' +
                    ", message='" + message + '\'' +
                    '}';
        }
    }

    public static class Exception
            extends RuntimeException
    {
        private final Object instance;
        private final List<Violation> violations;

        public Exception(Object instance, List<Violation> violations)
        {
            this.instance = instance;
            this.violations = violations;
        }

        @Override
        public String toString()
        {
            return "Exception{" +
                    "instance=" + instance +
                    ", violations=" + violations +
                    '}';
        }
    }

    public static <T> T validateInstance(T instance)
    {
        List<Violation> violations = VALIDATOR.validate(instance).stream().map(v -> new Violation(v.getPropertyPath().toString(), v.getMessage())).collect(toImmutableList());
        if (!violations.isEmpty()) {
            throw new Exception(instance, violations);
        }
        return instance;
    }
}
