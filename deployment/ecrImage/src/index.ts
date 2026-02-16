import * as pulumi from '@pulumi/pulumi';

import { ImageManagement } from './image';

const config = new pulumi.Config();

const image = new ImageManagement('dmnd-client-image', {
  appName: 'client',
  dockerContext: '../../',
  dockerfile: '../../Dockerfile',
  imageTag: process.env.IMAGE_VERSION,
});

export { image };
