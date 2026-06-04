import 'package:common/model/device.dart';
import 'package:common/src/task/discovery/http_target_discovery.dart';
import 'package:common/util/task_runner.dart';
import 'package:logging/logging.dart';
import 'package:refena/refena.dart';

final _logger = Logger('HttpScanDiscovery');

final httpScanDiscoveryProvider = ViewProvider((ref) {
  return HttpScanDiscoveryService(
    targetedDiscoveryService: ref.accessor(httpTargetDiscoveryProvider),
  );
});

Map<String, TaskRunner> _runners = {};

class HttpScanDiscoveryService {
  final StateAccessor<HttpTargetDiscoveryService> _targetedDiscoveryService;

  HttpScanDiscoveryService({
    required StateAccessor<HttpTargetDiscoveryService> targetedDiscoveryService,
  }) : _targetedDiscoveryService = targetedDiscoveryService;

  Stream<Device> getStream({required String networkInterface, required int port, required bool https}) {
    // Scan the whole /16 range (e.g. 172.18.0.0 - 172.18.255.255) instead of just
    // the local /24, so devices on a different third octet (e.g. phone on
    // 172.18.9.x while the PC is on 172.18.80.x) can still be discovered.
    final prefix = networkInterface.split('.').take(2).join('.');
    final ipList = <String>[];
    for (var third = 0; third < 256; third++) {
      for (var fourth = 0; fourth < 256; fourth++) {
        final ip = '$prefix.$third.$fourth';
        if (ip != networkInterface) {
          ipList.add(ip);
        }
      }
    }
    _runners[networkInterface]?.stop();
    _runners[networkInterface] = TaskRunner<Device?>(
      initialTasks: List.generate(
        ipList.length,
        (index) => () async => _doRequest(ipList[index], port, https),
      ),
      // Higher concurrency because a /16 scan covers 65536 addresses.
      concurrency: 256,
    );

    return _runners[networkInterface]!.stream.where((device) => device != null).cast<Device>();
  }

  Stream<Device> getFavoriteStream({required List<(String, int)> devices, required bool https}) {
    final runner = TaskRunner<Device?>(
      initialTasks: List.generate(
        devices.length,
        (index) => () async {
          final device = devices[index];
          return _doRequest(device.$1, device.$2, https);
        },
      ),
      concurrency: 50,
    );

    return runner.stream.where((device) => device != null).cast<Device>();
  }

  Future<Device?> _doRequest(String currentIp, int port, bool https) async {
    _logger.fine('Requesting $currentIp');
    final device = await _targetedDiscoveryService.state.discover(
      ip: currentIp,
      port: port,
      https: https,
      onError: null,
    );
    if (device != null) {
      _logger.info('[DISCOVER/TCP] ${device.alias} (${device.ip}, model: ${device.deviceModel})');
    }

    return device;
  }
}

