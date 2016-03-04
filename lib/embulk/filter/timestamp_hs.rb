Embulk::JavaPlugin.register_filter(
  "timestamp_hs", "org.embulk.filter.timestamp_hs.TimestampHsFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
