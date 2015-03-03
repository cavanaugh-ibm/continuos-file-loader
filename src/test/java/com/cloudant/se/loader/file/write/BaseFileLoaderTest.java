package com.cloudant.se.loader.file.write;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.junit.Ignore;
import org.junit.Test;

public class BaseFileLoaderTest {
    @Test
    public void testGetContentsAsMap() {
        try {
            BaseFileLoaderTestImpl writer = getWriter(true, Paths.get("src/test/resources/1.json"));
            Map<String, Object> map = writer.getContentsAsMap();

            for (int i = 1; i <= 10; i++) {
                assertEquals(map.get("field" + i), "value" + i);
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testGetId() throws ConfigurationException, IOException {
        // Test title processing
        BaseFileLoaderTestImpl writer = getWriter(true, Paths.get("src/test/resources/1.json"));
        Map<String, Object> map = writer.getContentsAsMap();
        String id = writer.getId(map);
        assertEquals("1", id);

        //
        // Test field processing
        writer = getWriter(false, Paths.get("src/test/resources/1.json"));
        map = writer.getContentsAsMap();
        id = writer.getId(map);
        assertEquals("subvalue", id);
    }

    @Test
    @Ignore
    public void testMoveFile() {
        fail("Not yet implemented");
    }

    private BaseFileLoaderTestImpl getWriter(boolean useTitle, Path path) throws ConfigurationException {
        if (useTitle) {
            return new BaseFileLoaderTestImpl(null, null, null, path, true, false, "NOT_USED");
        } else {
            return new BaseFileLoaderTestImpl(null, null, null, path, false, false, "NOT_USED", "submap/subkey");
        }
    }

}
