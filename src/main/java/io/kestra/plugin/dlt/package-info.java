@PluginSubGroup(
    description = "This sub-group of plugins contains tasks to interact with dlt (data load tool).\n" +
        "dlt is an open-source library for declaratively loading data from APIs, databases, and files into data warehouses and lakes.",
    categories = { PluginSubGroup.PluginCategory.INGESTION, PluginSubGroup.PluginCategory.TRANSFORMATION }
)
package io.kestra.plugin.dlt;

import io.kestra.core.models.annotations.PluginSubGroup;