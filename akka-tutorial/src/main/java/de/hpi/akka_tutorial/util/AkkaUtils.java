package de.hpi.akka_tutorial.util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Utilities for Akka.
 */
public class AkkaUtils {

	/**
	 * Binding to replace variables in our pimped {@code .conf} files.
	 */
	private static class VariableBinding {

		private final String pattern, value;

		private VariableBinding(String variableName, Object value) {
			this.pattern = Pattern.quote("$" + variableName);
			this.value = Objects.toString(value);
		}

		private String apply(String str) {
			return str.replaceAll(this.pattern, this.value);
		}
	}

	/**
	 * Load a {@link Config}.
	 *
	 * @param resource the path of the config resource
	 * @return the {@link Config}
	 */
	private static Config loadConfig(String resource, VariableBinding... bindings) {
		try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(resource)) {
			if (in == null) {
				throw new FileNotFoundException("Could not get the resource " + resource);
			}
			Stream<String> content = new BufferedReader(new InputStreamReader(in)).lines();
			for (VariableBinding binding : bindings) {
				content = content.map(binding::apply);
			}
			String result = content.collect(Collectors.joining("\n"));
			return ConfigFactory.parseString(result);
		} catch (IOException e) {
			throw new IllegalStateException("Could not load resource " + resource);
		}
	}


	public static Config createRemoteAkkaConfig(String host, int port) {
		Config baseConfig = loadConfig("akka-tutorial/base.conf");
		Config remoteConfig = loadConfig(
				"akka-tutorial/remote.conf",
				new VariableBinding("host", host),
				new VariableBinding("port", port)
		);
		return remoteConfig.withFallback(baseConfig);
	}

}
