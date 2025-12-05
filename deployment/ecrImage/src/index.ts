import * as pulumi from '@pulumi/pulumi';

import { ImageManagement } from './image';

const config = new pulumi.Config();

const image = new ImageManagement('dmnd-client-image', {
  env: 'prod',
  appName: 'dmnd-client',
  dockerContext: '../../',
  dockerfile: '../../Dockerfile',
  imageTag: config.require('version'),
});

export { image };
