/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
