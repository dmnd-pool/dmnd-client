import * as aws from '@pulumi/aws';
import * as pulumi from '@pulumi/pulumi';
import * as docker from '@pulumi/docker';

export interface ImageManagementArgs {
  env: string;
  appName: string;
  dockerContext: string;
  dockerfile: string;
  imageTag?: string;
}

export interface ImageManagementOutputs {
  repositoryUrl: pulumi.Output<string>;
  imageUri: pulumi.Output<string>;
  imageDigest: pulumi.Output<string>;
  repositoryName: pulumi.Output<string>;
}

export class ImageManagement extends pulumi.ComponentResource implements ImageManagementOutputs {
  public readonly repositoryUrl: pulumi.Output<string>;
  public readonly imageUri: pulumi.Output<string>;
  public readonly imageDigest: pulumi.Output<string>;
  public readonly repositoryName: pulumi.Output<string>;

  constructor(name: string, args: ImageManagementArgs, opts?: pulumi.ComponentResourceOptions) {
    super('custom:component:ImageManagement', name, {}, opts);

    const {
      env,
      appName,
      dockerContext,
      dockerfile,
      imageTag = 'latest'
    } = args;

    const repo = new aws.ecr.Repository(`${env}-${appName}-repo`, {
      name: `${env}/${appName}`,
      imageTagMutability: 'IMMUTABLE',
      imageScanningConfiguration: {
        scanOnPush: false,
      },
    }, { parent: this });

    new aws.ecr.LifecyclePolicy(`${env}-${appName}-lifecycle`, {
      repository: repo.name,
      policy: JSON.stringify({
        rules: [
          {
            rulePriority: 1,
            description: 'Keep only the last 10 images',
            selection: {
              tagStatus: 'any',
              countType: 'imageCountMoreThan',
              countNumber: 10,
            },
            action: {
              type: 'expire',
            },
          },
        ],
      }),
    }, { parent: this });

    const image = new docker.Image(`${env}-${appName}-image`, {
      imageName: pulumi.interpolate`${repo.repositoryUrl}:${imageTag}`,
      build: {
        context: dockerContext,
        dockerfile: dockerfile,
        platform: 'linux/amd64',
        args: {
          BUILD_DATE: new Date().toISOString(),
        },
      },
      registry: repo.registryId.apply(async _registryId => {
        const creds = await aws.ecr.getAuthorizationToken({});
        const [username, password] = Buffer.from(creds.authorizationToken, 'base64')
          .toString()
          .split(':');
        return {
          server: repo.repositoryUrl,
          username,
          password,
        };
      }),
      skipPush: false,
    }, { parent: this });

    this.repositoryUrl = repo.repositoryUrl;
    this.imageUri = image.imageName;
    this.imageDigest = image.repoDigest;
    this.repositoryName = repo.name;

    this.registerOutputs({
      repositoryUrl: this.repositoryUrl,
      imageUri: this.imageUri,
      imageDigest: this.imageDigest,
      repositoryName: this.repositoryName,
    });
  }
}
