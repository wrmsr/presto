package com.wrmsr.presto.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;

import static com.wrmsr.presto.util.Serialization.OBJECT_MAPPER;
import static com.wrmsr.presto.util.Serialization.roundTrip;
import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableList;

/*
sql
connector + name
script + name

list

string
file
*/
public class ExecConfig
        implements MainConfigNode<ExecConfig>
{
    @JsonTypeInfo(
            use = JsonTypeInfo.Id.NAME,
            include = JsonTypeInfo.As.WRAPPER_OBJECT)
    @JsonSubTypes({
            @JsonSubTypes.Type(value = StringVerb.class, name = "string"),
            @JsonSubTypes.Type(value = FileVerb.class, name = "file"),
    })
    public static abstract class Verb
    {
    }

    public static final class StringVerb
            extends Verb
    {
        protected final String statement;

        @JsonCreator
        public static StringVerb valueOf(String statement)
        {
            return new StringVerb(statement);
        }

        public StringVerb(String statement)
        {
            this.statement = statement;
        }

        @JsonValue
        public String getStatement()
        {
            return statement;
        }
    }

    public static final class FileVerb
            extends Verb
    {
        protected final String path;

        @JsonCreator
        public static FileVerb valueOf(String path)
        {
            return new FileVerb(path);
        }

        public FileVerb(String path)
        {
            this.path = path;
        }

        @JsonValue
        public String getPath()
        {
            return path;
        }
    }

    public static final class VerbList
    {
        @JsonCreator
        public static VerbList valueOf(Object object)
        {
            ObjectMapper mapper = OBJECT_MAPPER.get();
            List children;
            if (object instanceof String) {
                children = ImmutableList.of(object);
            }
            else if (object instanceof List) {
                children = (List) object;
            }
            else {
                throw new IllegalArgumentException();
            }

            ImmutableList.Builder<Verb> builder = ImmutableList.builder();
            for (Object child : children) {
                builder.add(roundTrip(mapper, child, Verb.class));
            }
            return new VerbList(builder.build());
        }

        protected final List<Verb> verbs;

        public VerbList(List<Verb> verbs)
        {
            this.verbs = ImmutableList.copyOf(verbs);
        }

        @JsonValue
        public List<Verb> getVerbs()
        {
            return verbs;
        }
    }

    @JsonTypeInfo(
            use = JsonTypeInfo.Id.NAME,
            include = JsonTypeInfo.As.WRAPPER_OBJECT)
    @JsonSubTypes({
            @JsonSubTypes.Type(value = SqlSubject.class, name = "sql"),
            @JsonSubTypes.Type(value = ConnectorSubject.class, name = "connector"),
            @JsonSubTypes.Type(value = ScriptSubject.class, name = "script"),
    })
    public static abstract class Subject
    {
    }

    public static final class SqlSubject
            extends Subject
    {
        protected final VerbList verbs;

        @JsonCreator
        public static SqlSubject valueOf(VerbList verbs)
        {
            return new SqlSubject(verbs);
        }

        public SqlSubject(VerbList verbs)
        {
            this.verbs = verbs;
        }

        @JsonValue
        public VerbList getVerbs()
        {
            return verbs;
        }
    }

    public static final class ConnectorSubject
            extends Subject
    {
        protected final Map<String, VerbList> verbs;

        @JsonCreator
        public static ConnectorSubject valueOf(Map<String, VerbList> verbs)
        {
            return new ConnectorSubject(verbs);
        }

        public ConnectorSubject(Map<String, VerbList> verbs)
        {
            this.verbs = verbs;
        }

        @JsonValue
        public Map<String, VerbList> getVerbs()
        {
            return verbs;
        }
    }

    public static final class ScriptSubject
            extends Subject
    {
        protected final Map<String, VerbList> verbs;

        @JsonCreator
        public static ScriptSubject valueOf(Map<String, VerbList> verbs)
        {
            return new ScriptSubject(verbs);
        }

        public ScriptSubject(Map<String, VerbList> verbs)
        {
            this.verbs = verbs;
        }

        @JsonValue
        public Map<String, VerbList> getVerbs()
        {
            return verbs;
        }
    }

    public static final class SubjectList
    {
        @JsonCreator
        public static SubjectList valueOf(Object object)
        {
            ObjectMapper mapper = OBJECT_MAPPER.get();
            if (object instanceof String) {
                return new SubjectList(
                        ImmutableList.of(
                                new SqlSubject(
                                        new VerbList(
                                                ImmutableList.of(
                                                        new StringVerb((String) object))))));
            }
            else if (object instanceof List) {
                return new SubjectList(
                        ImmutableList.of(
                                new SqlSubject(
                                        new VerbList(
                                                ((List<String>) object).stream().map(o -> new StringVerb(o)).collect(toImmutableList())))));
            }
            else if (object instanceof Map) {
                /*
                try {
                    mapper.readValue(mapper.writeValueAsString(object), new TypeReference<Map<String>>() {
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
                */
                throw new IllegalArgumentException();
            }
            else {
                throw new IllegalArgumentException();
            }
        }

        protected final List<Subject> subjects;

        public SubjectList(List<Subject> subjects)
        {
            this.subjects = ImmutableList.copyOf(subjects);
        }

        @JsonValue
        public List<Subject> getSubjects()
        {
            return subjects;
        }
    }

    @JsonCreator
    public static ExecConfig valueOf(Object object)
    {
        return new ExecConfig(SubjectList.valueOf(object));
    }

    protected final SubjectList subjects;

    public ExecConfig(SubjectList subjects)
    {
        this.subjects = subjects;
    }

    @JsonValue
    public SubjectList getSubjects()
    {
        return subjects;
    }

    public static ExecConfig newDefault()
    {
        return new ExecConfig(new SubjectList(ImmutableList.of()));
    }
}
