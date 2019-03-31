def get_services(config_path='config'):
    services = []
    if os.path.exists('%s/SERVICES' % config_path):
        services += open('%s/SERVICES' % config_path).readlines()
    services += [os.path.basename(os.path.splitext(path)[0]) for path in glob.glob('%s/*.service' % config_path)]
    return sorted(set(services))

